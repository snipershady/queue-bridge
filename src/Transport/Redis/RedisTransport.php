<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis;

use DateTimeImmutable;
use InvalidArgumentException;
use JsonException;
use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use QueueBridge\Codec\EnvelopeCodec;
use QueueBridge\Codec\EnvelopeCodecInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\BatchSendException;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\SerializationException;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Transport\Capability\BatchAckAwareInterface;
use QueueBridge\Transport\Capability\BrowseAwareInterface;
use QueueBridge\Transport\Capability\DeadLetterAwareInterface;
use QueueBridge\Transport\Capability\MaintenanceAwareInterface;
use QueueBridge\Transport\Capability\PurgeAwareInterface;
use QueueBridge\Transport\Capability\SetupAwareInterface;
use QueueBridge\Transport\Capability\StatsAwareInterface;
use QueueBridge\Transport\DeadLetter;
use QueueBridge\Transport\MaintenanceReport;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueuedMessageState;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\Feature\GroupInfo;
use QueueBridge\Transport\Redis\Feature\RedisFeatures;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;

/**
 * Transport su Redis Streams con un consumer group.
 *
 * - Ack, retry, reject e release sono script Lua atomici che controllano la proprietà della consegna.
 * - Gli orfani vengono rimessi in coda (non assegnati a chi fa manutenzione), con il totale delle consegne
 *   nel campo `deliveries`; su Redis >= 8.4 li recupera direttamente XREADGROUP ... CLAIM.
 * - I differiti stanno in un sorted set e vengono promossi a ogni receive() e in maintain().
 * - Le entry non decodificabili vanno in DLQ con FailureKind::Serialization e non raggiungono il worker.
 * - purge() svuota a pagine (uno script atomico per pagina) fino all'ultima entry presente all'inizio:
 *   non blocca Redis con backlog grandi e non insegue i messaggi pubblicati nel frattempo.
 *
 * Complessità dei metodi: t = byte di `text`, h = byte degli header, P = consegne pendenti, d = differiti,
 * T = scaglioni di attesa (RabbitMQ). "Round trip" = scambi di rete con il backend.
 *
 * @api
 */
final readonly class RedisTransport implements TransportInterface, SetupAwareInterface, MaintenanceAwareInterface, StatsAwareInterface, DeadLetterAwareInterface, BatchAckAwareInterface, PurgeAwareInterface, BrowseAwareInterface
{
    public const string FIELD_DELIVERIES = 'deliveries';

    private const string FIELD_FAILURE_KIND = 'failure_kind';

    private const string FIELD_FAILURE_ERROR_CLASS = 'failure_error_class';

    private const string FIELD_FAILURE_MESSAGE = 'failure_message';

    private const string FIELD_FAILURE_AT = 'failure_at';

    private const string FIELD_FAILURE_ORIGINAL_ID = 'failure_original_id';

    private const int PROMOTE_LIMIT = 100;

    private const int ORPHAN_PAGE = 100;

    private const int ORPHAN_MAX_PAGES = 10;

    private const int DEAD_LETTER_SCAN_PAGE = 500;

    private const int PURGE_PAGE = 500;

    private RedisKeys $keys;

    private ScriptRunner $scripts;

    /**
     * Complessità: O(1).
     *
     * @param non-empty-string $consumer nome univoco del consumer nel gruppo (uno per processo)
     *
     * @throws InvalidArgumentException se $consumer è vuoto
     */
    public function __construct(
        private RedisClientInterface $client,
        private RedisFeatures $features,
        private string $consumer,
        private QueueConfig $config,
        private ClockInterface $clock,
        private EnvelopeCodecInterface $codec = new EnvelopeCodec(),
        private LoggerInterface $logger = new NullLogger(),
    ) {
        if ('' === \trim($consumer)) {
            throw new InvalidArgumentException('Consumer name must not be empty.');
        }

        $this->keys = new RedisKeys($config);
        $this->scripts = new ScriptRunner($client);
    }

    /**
     * Complessità: O(1), un round trip.
     */
    #[Override]
    public function setup(): void
    {
        try {
            $this->client->execute(['XGROUP', 'CREATE', $this->keys->stream(), $this->keys->group(), '0', 'MKSTREAM']);
        } catch (CommandFailedException $commandFailedException) {
            if (!\str_contains($commandFailedException->getMessage(), 'BUSYGROUP')) {
                throw $commandFailedException;
            }
        }
    }

    /**
     * Complessità: O(t + h), un round trip; lato server XADD O(1), ZADD O(log d) per i differiti (d = differiti).
     */
    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        $this->client->execute($this->sendCommand($message, $delayMs));
    }

    /**
     * Complessità: O(n·(t + h)), n = messaggi; un round trip (pipeline).
     */
    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        $commands = [];
        $ids = [];
        foreach ($envelopes as $envelope) {
            $commands[] = $this->sendCommand($envelope->message, $envelope->delayMs);
            $ids[] = $envelope->message->id;
        }

        if ([] === $commands) {
            return;
        }

        $sent = [];
        $failed = [];
        $firstError = null;
        foreach ($this->client->pipeline($commands) as $index => $reply) {
            if ($reply instanceof CommandFailedException) {
                $failed[] = $ids[$index];
                $firstError ??= $reply;

                continue;
            }

            $sent[] = $ids[$index];
        }

        if ([] !== $failed) {
            throw new BatchSendException($sent, $failed, $firstError);
        }
    }

    /**
     * Complessità: O(k·(t + h)), k = consegne restituite; due round trip (promozione dei differiti e XREADGROUP), più
     * uno per ogni entry non decodificabile spostata in DLQ. Lato server la promozione è O(p log d), p = differiti
     * promossi (al più 100).
     */
    #[Override]
    public function receive(int $max = 1, int $timeoutMs = 1000): array
    {
        if ($max < 1) {
            throw new InvalidArgumentException(\sprintf('Max must be >= 1, got %d.', $max));
        }

        if ($timeoutMs < 0) {
            throw new InvalidArgumentException(\sprintf('Timeout must be >= 0 ms, got %d.', $timeoutMs));
        }

        $this->promoteDelayed();

        $command = ['XREADGROUP', 'GROUP', $this->keys->group(), $this->consumer, 'COUNT', (string) $max];
        if ($timeoutMs > 0) {
            $command[] = 'BLOCK';
            $command[] = (string) $timeoutMs;
        }

        array_push($command, ...$this->features->idleClaim->readArguments($this->config->claimIdleMs));
        $command[] = 'STREAMS';
        $command[] = $this->keys->stream();
        $command[] = '>';

        try {
            $reply = $this->client->execute($command);
        } catch (CommandFailedException $commandFailedException) {
            if (!\str_contains($commandFailedException->getMessage(), 'NOGROUP')) {
                throw $commandFailedException;
            }

            $this->setup();
            $reply = $this->client->execute($command);
        }

        return null === $reply ? [] : $this->deliveries($reply);
    }

    /**
     * Complessità: O(1), un round trip; lato server O(log P), P = consegne pendenti (verifica della proprietà).
     */
    #[Override]
    public function ack(Delivery $delivery): void
    {
        $this->ackBatch([$delivery]);
    }

    /**
     * Complessità: O(n), n = consegne; un round trip; lato server O(n log P), P = consegne pendenti.
     */
    #[Override]
    public function ackBatch(array $deliveries): void
    {
        if ([] === $deliveries) {
            return;
        }

        $this->scripts->run(
            RedisScripts::ack(),
            [$this->keys->stream()],
            [$this->keys->group(), $this->consumer, $this->features->ack->mode(), ...array_map(static fn (Delivery $delivery): string => $delivery->receipt, $deliveries)],
        );
    }

    /**
     * Complessità: O(t + h), un round trip (script atomico); lato server O(log P + log d).
     */
    #[Override]
    public function retry(Delivery $delivery, int $delayMs): void
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }

        $fields = $this->codec->encode($delivery->message->withIncrementedAttempts());

        if ($delayMs > 0) {
            $this->settle($delivery->receipt, 'delayed', (string) ($this->nowMs() + $delayMs), $this->member($fields), []);

            return;
        }

        $this->settle($delivery->receipt, 'stream', '0', '', $fields);
    }

    /**
     * Complessità: O(t + h), un round trip (script atomico); lato server O(log P) più il trim approssimato della DLQ.
     */
    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $this->settle($delivery->receipt, 'dlq', (string) $this->config->dlqMaxLength, '', [
            ...$this->codec->encode($delivery->message),
            ...$this->failureFields($reason),
        ]);
    }

    /**
     * Complessità: O(t + h), un round trip (script atomico); lato server O(log P).
     */
    #[Override]
    public function release(Delivery $delivery): void
    {
        $this->settle($delivery->receipt, 'stream', '0', '', [
            ...$this->codec->encode($delivery->message),
            self::FIELD_DELIVERIES => (string) $delivery->deliveryCount,
        ]);
    }

    /**
     * Complessità: O(1), un round trip.
     */
    #[Override]
    public function ping(): void
    {
        $this->client->execute(['PING']);
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function capabilities(): TransportCapabilities
    {
        return new TransportCapabilities(
            orphanRecovery: OrphanRecovery::Explicit,
            nativeDelay: true,
            batchReceive: true,
            batchAck: true,
            deadLetter: true,
            consumerLag: $this->features->capabilities->groupLag,
            deliveryCount: true,
        );
    }

    /**
     * Complessità: O(p + o), p = differiti scaduti, o = orfani: ⌈(p + 1) / 100⌉ round trip per la promozione, al più 20
     * per gli orfani (1000 per chiamata) e al più 3 per il trim.
     */
    #[Override]
    public function maintain(): MaintenanceReport
    {
        $promoted = 0;
        do {
            $batch = $this->promoteDelayed();
            $promoted += $batch;
        } while (self::PROMOTE_LIMIT === $batch);

        return new MaintenanceReport(
            promotedDelayed: $promoted,
            recoveredOrphans: $this->recoverOrphans(),
            trimmed: $this->features->trim->trim($this->client, $this->keys),
        );
    }

    /**
     * Complessità: O(g), g = consumer group; 4 round trip.
     */
    #[Override]
    public function stats(): QueueStats
    {
        $group = GroupInfo::find($this->client, $this->keys);
        $streamLength = RedisReply::int($this->client->execute(['XLEN', $this->keys->stream()]), 'XLEN');
        $inFlight = null === $group ? 0 : RedisReply::int($group['pending'] ?? null, 'XINFO GROUPS pending');

        return new QueueStats(
            ready: \max(0, $streamLength - $inFlight),
            inFlight: $inFlight,
            delayed: RedisReply::int($this->client->execute(['ZCARD', $this->keys->delayed()]), 'ZCARD'),
            deadLetters: RedisReply::int($this->client->execute(['XLEN', $this->keys->deadLetters()]), 'XLEN'),
            lag: null === $group ? null : $this->features->lag->lag($group),
            consumers: null === $group ? null : RedisReply::int($group['consumers'] ?? null, 'XINFO GROUPS consumers'),
        );
    }

    /**
     * Complessità: O(limit·(t + h)), un round trip.
     */
    #[Override]
    public function deadLetters(int $limit = 100): array
    {
        $this->assertLimit($limit);
        $entries = RedisReply::list($this->client->execute(['XRANGE', $this->keys->deadLetters(), '-', '+', 'COUNT', (string) $limit]), 'XRANGE');

        $deadLetters = [];
        foreach ($entries as $entry) {
            $deadLetter = $this->deadLetter(RedisReply::fields(RedisReply::list($entry, 'XRANGE entry')[1] ?? null, 'XRANGE fields'));
            if ($deadLetter instanceof DeadLetter) {
                $deadLetters[] = $deadLetter;
            }
        }

        return $deadLetters;
    }

    /**
     * Complessità: O(1) round trip; lato server O(limit·f), f = campi per messaggio.
     */
    #[Override]
    public function requeueDeadLetters(int $limit = 100): int
    {
        $this->assertLimit($limit);

        return RedisReply::int(
            $this->scripts->run(RedisScripts::requeueDeadLetters(), [$this->keys->deadLetters(), $this->keys->stream()], ['first', (string) $limit]),
            'requeueDeadLetters',
        );
    }

    /**
     * Complessità: O(L), L = messaggi in DLQ prima di quello cercato: ⌈L / 500⌉ + 1 round trip.
     */
    #[Override]
    public function requeueDeadLetter(MessageId $id): bool
    {
        $start = '-';
        do {
            $entries = RedisReply::list(
                $this->client->execute(['XRANGE', $this->keys->deadLetters(), $start, '+', 'COUNT', (string) self::DEAD_LETTER_SCAN_PAGE]),
                'XRANGE',
            );

            foreach ($entries as $entry) {
                $entry = RedisReply::list($entry, 'XRANGE entry');
                $entryId = RedisReply::string($entry[0] ?? null, 'XRANGE id');
                $fields = RedisReply::fields($entry[1] ?? null, 'XRANGE fields');

                if (($fields[EnvelopeCodec::FIELD_ID] ?? null) === $id->value) {
                    return 1 === RedisReply::int(
                        $this->scripts->run(RedisScripts::requeueDeadLetters(), [$this->keys->deadLetters(), $this->keys->stream()], ['id', $entryId]),
                        'requeueDeadLetter',
                    );
                }

                $start = $this->nextId($entryId);
            }
        } while (self::DEAD_LETTER_SCAN_PAGE === \count($entries));

        return false;
    }

    /**
     * Complessità: O(1) round trip; lato server O(L), L = messaggi in DLQ.
     */
    #[Override]
    public function purgeDeadLetters(): int
    {
        return RedisReply::int($this->scripts->run(RedisScripts::purgeDeadLetters(), [$this->keys->deadLetters()], []), 'purgeDeadLetters');
    }

    /**
     * Complessità: O(n), n = entry dello stream presenti alla chiamata: ⌈n / 500⌉ + 2 round trip, ognuno breve (una
     * pagina) per non bloccare Redis.
     */
    #[Override]
    public function purge(): int
    {
        // Prima i differiti: quelli promossi nel frattempo finiscono nello stream entro l'ultimo ID letto sotto.
        $purged = RedisReply::int($this->scripts->run(RedisScripts::purgeDelayed(), [$this->keys->delayed()], []), 'purgeDelayed');

        $newest = RedisReply::list($this->client->execute(['XREVRANGE', $this->keys->stream(), '+', '-', 'COUNT', '1']), 'XREVRANGE');
        if ([] === $newest) {
            return $purged;
        }

        $end = RedisReply::string(RedisReply::list($newest[0], 'XREVRANGE entry')[0] ?? null, 'XREVRANGE id');
        $start = '-';
        do {
            $page = RedisReply::list(
                $this->scripts->run(RedisScripts::purgeReady(), [$this->keys->stream()], [$this->keys->group(), $start, $end, (string) self::PURGE_PAGE]),
                'purgeReady',
            );
            $purged += RedisReply::int($page[0] ?? null, 'purgeReady deleted');
            $scanned = RedisReply::int($page[1] ?? null, 'purgeReady scanned');
            if ($scanned > 0) {
                $start = $this->nextId(RedisReply::string($page[2] ?? null, 'purgeReady last id'));
            }
        } while (self::PURGE_PAGE === $scanned);

        return $purged;
    }

    /**
     * Complessità: O(limit·(t + h)), al più 3 round trip.
     */
    #[Override]
    public function browse(int $limit = 100): array
    {
        $this->assertLimit($limit);
        $entries = RedisReply::list($this->client->execute(['XRANGE', $this->keys->stream(), '-', '+', 'COUNT', (string) $limit]), 'XRANGE');

        $queued = [];
        $pending = $this->pendingIds($entries);
        foreach ($entries as $entry) {
            $entry = RedisReply::list($entry, 'XRANGE entry');
            $id = RedisReply::string($entry[0] ?? null, 'XRANGE id');
            $message = $this->decodeOrNull(RedisReply::fields($entry[1] ?? null, 'XRANGE fields'));
            if ($message instanceof Message) {
                $queued[] = new QueuedMessage($message, isset($pending[$id]) ? QueuedMessageState::InFlight : QueuedMessageState::Ready);
            }
        }

        if (\count($entries) >= $limit) {
            return $queued;
        }

        // [membro, score, membro, score, ...]: non una mappa, un membro numerico diventerebbe una chiave intera.
        $delayed = RedisReply::list(
            $this->client->execute(['ZRANGE', $this->keys->delayed(), '0', (string) ($limit - \count($entries) - 1), 'WITHSCORES']),
            'ZRANGE',
        );
        for ($i = 0, $count = \count($delayed) - 1; $i < $count; $i += 2) {
            $message = $this->decodeOrNull($this->memberFields(RedisReply::string($delayed[$i], 'ZRANGE member')));
            if ($message instanceof Message) {
                $dueMs = (int) RedisReply::string($delayed[$i + 1], 'ZRANGE score');
                $availableAt = new DateTimeImmutable(\sprintf('@%d.%03d', intdiv($dueMs, 1000), $dueMs % 1000));
                $queued[] = new QueuedMessage($message, QueuedMessageState::Delayed, $availableAt);
            }
        }

        return $queued;
    }

    /**
     * @return non-empty-list<string>
     */
    private function sendCommand(Message $message, int $delayMs): array
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }

        $fields = $this->codec->encode($message);
        if ($delayMs > 0) {
            return ['ZADD', $this->keys->delayed(), (string) ($this->nowMs() + $delayMs), $this->member($fields)];
        }

        return ['XADD', $this->keys->stream(), '*', ...$this->flatten($fields)];
    }

    /**
     * @return list<Delivery>
     */
    private function deliveries(mixed $reply): array
    {
        $now = $this->clock->now();
        $deliveries = [];

        foreach (RedisReply::list($reply, 'XREADGROUP') as $stream) {
            foreach (RedisReply::list(RedisReply::list($stream, 'XREADGROUP stream')[1] ?? null, 'XREADGROUP entries') as $entry) {
                $delivery = $this->delivery(RedisReply::list($entry, 'XREADGROUP entry'), $now);
                if ($delivery instanceof Delivery) {
                    $deliveries[] = $delivery;
                }
            }
        }

        return $deliveries;
    }

    /**
     * @param list<mixed> $entry [id, campi | nil] oppure, se reclamata con CLAIM, [id, campi | nil, inattività, consegne precedenti]
     */
    private function delivery(array $entry, DateTimeImmutable $now): ?Delivery
    {
        $id = RedisReply::string($entry[0] ?? null, 'XREADGROUP id');
        if (null === ($entry[1] ?? null)) {
            // Entry cancellata ma ancora referenziata nella PEL: non c'è nulla da consegnare.
            $this->ackIds([$id]);

            return null;
        }

        $fields = RedisReply::fields($entry[1], 'XREADGROUP fields');
        $previous = (int) ($fields[self::FIELD_DELIVERIES] ?? 0);
        $thisCopy = \array_key_exists(3, $entry) ? RedisReply::int($entry[3], 'XREADGROUP CLAIM deliveries') + 1 : 1;

        try {
            $message = $this->codec->decode($fields);
        } catch (SerializationException $serializationException) {
            $this->logger->error('Undecodable entry {id} moved to the dead letter queue: {error}', ['id' => $id, 'error' => $serializationException->getMessage()]);
            $this->settle($id, 'dlq', (string) $this->config->dlqMaxLength, '', [
                ...$fields,
                self::FIELD_FAILURE_KIND => FailureKind::Serialization->value,
                self::FIELD_FAILURE_ERROR_CLASS => SerializationException::class,
                self::FIELD_FAILURE_MESSAGE => $serializationException->getMessage(),
                self::FIELD_FAILURE_AT => $now->format('Uv'),
                self::FIELD_FAILURE_ORIGINAL_ID => $fields[EnvelopeCodec::FIELD_ID] ?? '',
            ]);

            return null;
        }

        return new Delivery($message, $id, $now, $previous + $thisCopy);
    }

    /**
     * @param array<string, string> $fields
     */
    private function deadLetter(array $fields): ?DeadLetter
    {
        try {
            $message = $this->codec->decode($fields);
            $kind = FailureKind::tryFrom($fields[self::FIELD_FAILURE_KIND] ?? '') ?? FailureKind::HandlerFailed;
            $errorClass = $fields[self::FIELD_FAILURE_ERROR_CLASS] ?? '';
            $failedAtMs = (int) ($fields[self::FIELD_FAILURE_AT] ?? 0);

            return new DeadLetter($message, new FailureReason(
                $kind,
                $message->id,
                new DateTimeImmutable(\sprintf('@%d.%03d', intdiv($failedAtMs, 1000), $failedAtMs % 1000)),
                $fields[self::FIELD_FAILURE_MESSAGE] ?? '',
                '' === $errorClass ? null : $errorClass,
            ));
        } catch (SerializationException) {
            // Entry corrotta: resta in DLQ, ispezionabile con XRANGE, ma non è un DeadLetter valido.
            return null;
        }
    }

    /**
     * ID delle entry di $entries che sono nella PEL del gruppo (in elaborazione).
     *
     * @param list<mixed> $entries risposta di XRANGE
     *
     * @return array<string, true>
     */
    private function pendingIds(array $entries): array
    {
        if ([] === $entries) {
            return [];
        }

        $first = RedisReply::string(RedisReply::list($entries[0], 'XRANGE entry')[0] ?? null, 'XRANGE id');
        $last = RedisReply::string(RedisReply::list($entries[\count($entries) - 1], 'XRANGE entry')[0] ?? null, 'XRANGE id');

        try {
            $reply = $this->client->execute(['XPENDING', $this->keys->stream(), $this->keys->group(), $first, $last, (string) \count($entries)]);
        } catch (CommandFailedException $commandFailedException) {
            if (!\str_contains($commandFailedException->getMessage(), 'NOGROUP')) {
                throw $commandFailedException;
            }

            return [];
        }

        $pending = [];
        foreach (RedisReply::list($reply, 'XPENDING') as $item) {
            $pending[RedisReply::string(RedisReply::list($item, 'XPENDING entry')[0] ?? null, 'XPENDING id')] = true;
        }

        return $pending;
    }

    /**
     * @param array<string, string> $fields
     */
    private function decodeOrNull(array $fields): ?Message
    {
        try {
            return $this->codec->decode($fields);
        } catch (SerializationException) {
            return null;
        }
    }

    /**
     * Campi di un membro del sorted set dei differiti (oggetto JSON {campo: valore}); vuoto se non valido.
     *
     * @return array<string, string>
     */
    private function memberFields(string $member): array
    {
        try {
            $decoded = json_decode($member, associative: true, depth: 2, flags: \JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return [];
        }

        if (!\is_array($decoded)) {
            return [];
        }

        $fields = [];
        foreach ($decoded as $name => $value) {
            if (!\is_string($value)) {
                return [];
            }

            $fields[(string) $name] = $value;
        }

        return $fields;
    }

    /**
     * @param array<string, string> $fields
     */
    private function settle(string $receipt, string $target, string $score, string $member, array $fields): void
    {
        $this->scripts->run(
            RedisScripts::settle(),
            [$this->keys->stream(), $this->keys->delayed(), $this->keys->deadLetters()],
            [$this->keys->group(), $this->consumer, $this->features->ack->mode(), $receipt, $target, $score, $member, ...$this->flatten($fields)],
        );
    }

    /**
     * @param list<string> $ids
     */
    private function ackIds(array $ids): void
    {
        $this->scripts->run(RedisScripts::ack(), [$this->keys->stream()], [$this->keys->group(), $this->consumer, $this->features->ack->mode(), ...$ids]);
    }

    private function promoteDelayed(): int
    {
        return RedisReply::int(
            $this->scripts->run(RedisScripts::promoteDelayed(), [$this->keys->delayed(), $this->keys->stream()], [(string) $this->nowMs(), (string) self::PROMOTE_LIMIT]),
            'promoteDelayed',
        );
    }

    private function recoverOrphans(): int
    {
        $idleClaim = $this->features->idleClaim;
        if ($idleClaim->claimsOnRead()) {
            return 0;
        }

        $recovered = 0;
        for ($page = 0; $page < self::ORPHAN_MAX_PAGES; ++$page) {
            $ids = $idleClaim->findIdle($this->client, $this->keys, $this->config->claimIdleMs, self::ORPHAN_PAGE);
            if ([] === $ids) {
                break;
            }

            $recovered += RedisReply::int($this->scripts->run(
                RedisScripts::requeueOrphans(),
                [$this->keys->stream()],
                [$this->keys->group(), $this->features->ack->mode(), (string) $this->config->claimIdleMs, ...$ids],
            ), 'requeueOrphans');

            if (\count($ids) < self::ORPHAN_PAGE) {
                break;
            }
        }

        return $recovered;
    }

    /**
     * @return array<string, string>
     */
    private function failureFields(FailureReason $failureReason): array
    {
        return [
            self::FIELD_FAILURE_KIND => $failureReason->kind->value,
            self::FIELD_FAILURE_ERROR_CLASS => $failureReason->errorClass ?? '',
            self::FIELD_FAILURE_MESSAGE => $failureReason->errorMessage,
            self::FIELD_FAILURE_AT => $failureReason->failedAt->format('Uv'),
            self::FIELD_FAILURE_ORIGINAL_ID => $failureReason->originalId->value,
        ];
    }

    /**
     * @param array<string, string> $fields
     */
    private function member(array $fields): string
    {
        return json_encode($fields, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES);
    }

    private function assertLimit(int $limit): void
    {
        if ($limit < 1) {
            throw new InvalidArgumentException(\sprintf('Limit must be >= 1, got %d.', $limit));
        }
    }

    private function nowMs(): int
    {
        return (int) $this->clock->now()->format('Uv');
    }

    /**
     * @param array<string, string> $fields
     *
     * @return list<string>
     */
    private function flatten(array $fields): array
    {
        $flat = [];
        foreach ($fields as $name => $value) {
            $flat[] = $name;
            $flat[] = $value;
        }

        return $flat;
    }

    /**
     * ID successivo per paginare XRANGE senza la sintassi esclusiva "(id" (Redis >= 6.2).
     */
    private function nextId(string $id): string
    {
        [$milliseconds, $sequence] = explode('-', $id) + [1 => '0'];

        return $milliseconds.'-'.((int) $sequence + 1);
    }
}
