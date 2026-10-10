<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis;

use DateTimeImmutable;
use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Codec\EnvelopeCodec;
use snipershady\Queue\Codec\EnvelopeCodecInterface;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\BatchSendException;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\SerializationException;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Transport\Capability\BatchAckAwareInterface;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
use snipershady\Queue\Transport\Capability\MaintenanceAwareInterface;
use snipershady\Queue\Transport\Capability\SetupAwareInterface;
use snipershady\Queue\Transport\Capability\StatsAwareInterface;
use snipershady\Queue\Transport\DeadLetter;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Feature\GroupInfo;
use snipershady\Queue\Transport\Redis\Feature\RedisFeatures;
use snipershady\Queue\Transport\Redis\Script\RedisScripts;
use snipershady\Queue\Transport\Redis\Script\ScriptRunner;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;

/**
 * Transport su Redis Streams con un consumer group.
 *
 * - Ack, retry, reject e release sono script Lua atomici che controllano la proprietà della consegna.
 * - Gli orfani vengono rimessi in coda (non assegnati a chi fa manutenzione), con il totale delle consegne
 *   nel campo `deliveries`; su Redis >= 8.4 li recupera direttamente XREADGROUP ... CLAIM.
 * - I differiti stanno in un sorted set e vengono promossi a ogni receive() e in maintain().
 * - Le entry non decodificabili vanno in DLQ con FailureKind::Serialization e non raggiungono il worker.
 *
 * @api
 */
final readonly class RedisTransport implements TransportInterface, SetupAwareInterface, MaintenanceAwareInterface, StatsAwareInterface, DeadLetterAwareInterface, BatchAckAwareInterface
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

    private RedisKeys $keys;

    private ScriptRunner $scripts;

    /**
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
        if ('' === trim($consumer)) {
            throw new InvalidArgumentException('Consumer name must not be empty.');
        }

        $this->keys = new RedisKeys($config);
        $this->scripts = new ScriptRunner($client);
    }

    #[Override]
    public function setup(): void
    {
        try {
            $this->client->execute(['XGROUP', 'CREATE', $this->keys->stream(), $this->keys->group(), '0', 'MKSTREAM']);
        } catch (CommandFailedException $commandFailedException) {
            if (!str_contains($commandFailedException->getMessage(), 'BUSYGROUP')) {
                throw $commandFailedException;
            }
        }
    }

    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        $this->client->execute($this->sendCommand($message, $delayMs));
    }

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
            if (!str_contains($commandFailedException->getMessage(), 'NOGROUP')) {
                throw $commandFailedException;
            }

            $this->setup();
            $reply = $this->client->execute($command);
        }

        return null === $reply ? [] : $this->deliveries($reply);
    }

    #[Override]
    public function ack(Delivery $delivery): void
    {
        $this->ackBatch([$delivery]);
    }

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

    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $this->settle($delivery->receipt, 'dlq', (string) $this->config->dlqMaxLength, '', [
            ...$this->codec->encode($delivery->message),
            ...$this->failureFields($reason),
        ]);
    }

    #[Override]
    public function release(Delivery $delivery): void
    {
        $this->settle($delivery->receipt, 'stream', '0', '', [
            ...$this->codec->encode($delivery->message),
            self::FIELD_DELIVERIES => (string) $delivery->deliveryCount,
        ]);
    }

    #[Override]
    public function ping(): void
    {
        $this->client->execute(['PING']);
    }

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

    #[Override]
    public function stats(): QueueStats
    {
        $group = GroupInfo::find($this->client, $this->keys);
        $streamLength = RedisReply::int($this->client->execute(['XLEN', $this->keys->stream()]), 'XLEN');
        $inFlight = null === $group ? 0 : RedisReply::int($group['pending'] ?? null, 'XINFO GROUPS pending');

        return new QueueStats(
            ready: max(0, $streamLength - $inFlight),
            inFlight: $inFlight,
            delayed: RedisReply::int($this->client->execute(['ZCARD', $this->keys->delayed()]), 'ZCARD'),
            deadLetters: RedisReply::int($this->client->execute(['XLEN', $this->keys->deadLetters()]), 'XLEN'),
            lag: null === $group ? null : $this->features->lag->lag($group),
            consumers: null === $group ? null : RedisReply::int($group['consumers'] ?? null, 'XINFO GROUPS consumers'),
        );
    }

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

    #[Override]
    public function requeueDeadLetters(int $limit = 100): int
    {
        $this->assertLimit($limit);

        return RedisReply::int(
            $this->scripts->run(RedisScripts::requeueDeadLetters(), [$this->keys->deadLetters(), $this->keys->stream()], ['first', (string) $limit]),
            'requeueDeadLetters',
        );
    }

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

    #[Override]
    public function purgeDeadLetters(): int
    {
        return RedisReply::int($this->scripts->run(RedisScripts::purgeDeadLetters(), [$this->keys->deadLetters()], []), 'purgeDeadLetters');
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
