<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq;

use Closure;
use DateTimeImmutable;
use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Codec\EnvelopeCodec;
use snipershady\Queue\Codec\EnvelopeCodecInterface;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\BackendException;
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
use snipershady\Queue\Transport\Capability\SetupAwareInterface;
use snipershady\Queue\Transport\Capability\StatsAwareInterface;
use snipershady\Queue\Transport\DeadLetter;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpChannelInterface;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpConnectorInterface;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpMessage;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;

/**
 * Transport su RabbitMQ (AMQP 0-9-1) con quorum queue (CLAUDE.md §10).
 *
 * - Gli orfani tornano in coda da soli quando il canale si chiude (OrphanRecovery::Native): nessuna manutenzione.
 * - Le ricevute sono `generazione:delivery-tag`: dopo la riapertura del canale quelle vecchie non hanno effetto.
 * - Differiti e retry passano per code di attesa a scaglioni (RetryTiers) con l'header `x-qb-due-at`.
 * - Retry e reject pubblicano la nuova copia, attendono la conferma del broker e solo dopo confermano
 *   l'originale: mai persi, al più duplicati.
 * - Le statistiche di "in elaborazione" sono quelle di questo consumer: AMQP non espone gli unacked degli altri.
 *
 * @api
 */
final class RabbitMqTransport implements TransportInterface, SetupAwareInterface, StatsAwareInterface, DeadLetterAwareInterface, BatchAckAwareInterface
{
    public const string HEADER_PREFIX = 'x-qb-';

    public const string HEADER_DUE_AT = 'x-qb-due-at';

    private const string HEADER_DELIVERY_COUNT = 'x-delivery-count';

    private const string FAILURE_KIND = 'x-qb-failure-kind';

    private const string FAILURE_ERROR_CLASS = 'x-qb-failure-error-class';

    private const string FAILURE_MESSAGE = 'x-qb-failure-message';

    private const string FAILURE_AT = 'x-qb-failure-at';

    private readonly Topology $topology;

    /** @var list<AmqpMessage> ricevuti dal broker e non ancora restituiti da receive() */
    private array $buffer = [];

    /** @var array<int, true> delivery tag restituiti da receive() e non ancora confermati */
    private array $handedOut = [];

    private ?int $consumingGeneration = null;

    /**
     * @throws InvalidArgumentException se $prefetch o $confirmTimeoutMs sono < 1
     */
    public function __construct(
        private readonly AmqpConnectorInterface $connector,
        QueueConfig $config,
        private readonly ClockInterface $clock,
        private readonly RetryTiers $retryTiers = new RetryTiers(),
        private readonly int $prefetch = 10,
        private readonly int $confirmTimeoutMs = 5000,
        private readonly EnvelopeCodecInterface $codec = new EnvelopeCodec(),
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
        if ($prefetch < 1 || $confirmTimeoutMs < 1) {
            throw new InvalidArgumentException(\sprintf('Prefetch and confirm timeout must be >= 1, got %d and %d.', $prefetch, $confirmTimeoutMs));
        }

        $this->topology = new Topology($config, $retryTiers);
    }

    #[Override]
    public function setup(): void
    {
        $this->withChannel(fn (AmqpChannelInterface $amqpChannel) => $this->topology->declare($amqpChannel));
    }

    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        $this->assertDelay($delayMs);
        $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($message, $delayMs): void {
            $this->publishMessage($amqpChannel, $this->codec->encode($message), $delayMs);
            if ([] !== $amqpChannel->confirm($this->confirmTimeoutMs)) {
                throw CommandFailedException::rejected('basic.publish', \sprintf('message %s was not confirmed by the broker', $message->id->value));
            }
        });
    }

    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        $ids = [];
        $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($envelopes, &$ids): void {
            foreach ($envelopes as $envelope) {
                $this->publishMessage($amqpChannel, $this->codec->encode($envelope->message), $envelope->delayMs);
                $ids[] = $envelope->message->id;
            }

            if ([] === $ids) {
                return;
            }

            $failed = $amqpChannel->confirm($this->confirmTimeoutMs);
            if ([] === $failed) {
                return;
            }

            $sent = array_values(array_filter($ids, static fn (MessageId $messageId): bool => !\in_array($messageId->value, $failed, strict: true)));
            $notSent = array_values(array_filter($ids, static fn (MessageId $messageId): bool => \in_array($messageId->value, $failed, strict: true)));

            throw new BatchSendException($sent, $notSent);
        });
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

        return $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($max, $timeoutMs): array {
            $this->ensureConsuming($amqpChannel);

            if ([] === $this->buffer) {
                $amqpChannel->wait($timeoutMs);
            }

            if ([] !== $this->buffer) {
                $this->drainPrefetched($amqpChannel, $max);
            }

            $deliveries = [];
            $now = $this->clock->now();
            while (\count($deliveries) < $max && [] !== $this->buffer) {
                $delivery = $this->toDelivery($amqpChannel, array_shift($this->buffer), $now);
                if ($delivery instanceof Delivery) {
                    $deliveries[] = $delivery;
                }
            }

            return $deliveries;
        });
    }

    #[Override]
    public function ack(Delivery $delivery): void
    {
        $tag = $this->ownedTag($delivery);
        if (null === $tag) {
            return;
        }

        $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($tag): void {
            $amqpChannel->ack($tag);
            unset($this->handedOut[$tag]);
        });
    }

    #[Override]
    public function ackBatch(array $deliveries): void
    {
        foreach ($deliveries as $delivery) {
            $this->ack($delivery);
        }
    }

    #[Override]
    public function retry(Delivery $delivery, int $delayMs): void
    {
        $this->assertDelay($delayMs);
        $this->republishThenAck($delivery, function (AmqpChannelInterface $amqpChannel) use ($delivery, $delayMs): void {
            $fields = $this->codec->encode($delivery->message->withIncrementedAttempts());
            if ($delayMs > 0) {
                $this->publishDelayed($amqpChannel, $fields, $this->nowMs() + $delayMs);

                return;
            }

            $amqpChannel->publish($this->toAmqp($fields), '', $this->topology->queue);
        });
    }

    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $this->republishThenAck($delivery, function (AmqpChannelInterface $amqpChannel) use ($delivery, $reason): void {
            $amqpChannel->publish($this->toAmqp($this->codec->encode($delivery->message), $this->failureHeaders($reason)), '', $this->topology->deadLetterQueue);
        });
    }

    #[Override]
    public function release(Delivery $delivery): void
    {
        $tag = $this->ownedTag($delivery);
        if (null === $tag) {
            return;
        }

        $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($tag): void {
            $amqpChannel->nack($tag, requeue: true);
            unset($this->handedOut[$tag]);
        });
    }

    #[Override]
    public function ping(): void
    {
        $this->withChannel(static fn (AmqpChannelInterface $amqpChannel): bool => $amqpChannel->isOpen());
    }

    #[Override]
    public function capabilities(): TransportCapabilities
    {
        return new TransportCapabilities(
            orphanRecovery: OrphanRecovery::Native,
            nativeDelay: true,
            batchReceive: true,
            batchAck: true,
            deadLetter: true,
            consumerLag: false,
            deliveryCount: true,
        );
    }

    /**
     * Chiude il canale: i messaggi ricevuti e non confermati tornano in coda. Il prossimo uso lo riapre.
     */
    public function close(): void
    {
        $this->connector->reset();
        $this->forgetChannel();
    }

    #[Override]
    public function stats(): QueueStats
    {
        return $this->withChannel(function (AmqpChannelInterface $amqpChannel): QueueStats {
            if ($this->consumingGeneration === $this->connector->generation()) {
                // I messaggi già inviati a questo consumer non sono più "pronti" per il broker: si contano nel buffer.
                $this->drainPrefetched($amqpChannel, $this->prefetch);
            }

            try {
                [$ready, $consumers] = $amqpChannel->declareQueue($this->topology->queue, passive: true);
                $delayed = 0;
                foreach ($this->topology->retryQueues() as $retryQueue) {
                    $delayed += $amqpChannel->declareQueue($retryQueue, passive: true)[0];
                }

                $deadLetters = $amqpChannel->declareQueue($this->topology->deadLetterQueue, passive: true)[0];
            } catch (CommandFailedException) {
                // Topologia non ancora dichiarata: la dichiarazione passiva ha chiuso il canale.
                $this->close();

                return new QueueStats(0, 0, 0, 0);
            }

            return new QueueStats(
                ready: $ready + \count($this->buffer),
                inFlight: \count($this->handedOut),
                delayed: $delayed,
                deadLetters: $deadLetters,
                consumers: $consumers,
            );
        });
    }

    #[Override]
    public function deadLetters(int $limit = 100): array
    {
        $this->assertLimit($limit);

        return $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($limit): array {
            $taken = $this->takeDeadLetters($amqpChannel, $limit);
            $deadLetters = [];
            foreach ($taken as $amqpMessage) {
                $deadLetter = $this->toDeadLetter($amqpMessage);
                if ($deadLetter instanceof DeadLetter) {
                    $deadLetters[] = $deadLetter;
                }
            }

            $this->returnAll($amqpChannel, $taken);

            return $deadLetters;
        });
    }

    #[Override]
    public function requeueDeadLetters(int $limit = 100): int
    {
        $this->assertLimit($limit);

        return $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($limit): int {
            $requeue = [];
            $keep = [];
            foreach ($this->takeDeadLetters($amqpChannel, $limit) as $amqpMessage) {
                if ($this->toDeadLetter($amqpMessage) instanceof DeadLetter) {
                    $requeue[] = $amqpMessage;
                } else {
                    $keep[] = $amqpMessage;
                }
            }

            $this->returnAll($amqpChannel, $keep);

            return $this->requeue($amqpChannel, $requeue);
        });
    }

    #[Override]
    public function requeueDeadLetter(MessageId $id): bool
    {
        return $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($id): bool {
            $others = [];
            $found = null;
            while (($amqpMessage = $amqpChannel->get($this->topology->deadLetterQueue)) instanceof AmqpMessage) {
                if ($amqpMessage->messageId === $id->value && $this->toDeadLetter($amqpMessage) instanceof DeadLetter) {
                    $found = $amqpMessage;

                    break;
                }

                $others[] = $amqpMessage;
            }

            $this->returnAll($amqpChannel, $others);

            return $found instanceof AmqpMessage && 1 === $this->requeue($amqpChannel, [$found]);
        });
    }

    #[Override]
    public function purgeDeadLetters(): int
    {
        return $this->withChannel(fn (AmqpChannelInterface $amqpChannel): int => $amqpChannel->purgeQueue($this->topology->deadLetterQueue));
    }

    private function toDelivery(AmqpChannelInterface $amqpChannel, AmqpMessage $amqpMessage, DateTimeImmutable $now): ?Delivery
    {
        $tag = $amqpMessage->deliveryTag ?? throw CommandFailedException::unexpectedReply('basic.deliver', 'a delivery tag');
        $fields = $this->fields($amqpMessage);

        $dueAt = (int) ($amqpMessage->headers[self::HEADER_DUE_AT] ?? 0);
        if ($dueAt > (int) $now->format('Uv')) {
            // Tornato da una coda di attesa prima della scadenza: rimandato per il tempo che manca.
            $this->publishDelayed($amqpChannel, $fields, $dueAt);
            $this->confirmed($amqpChannel);
            $amqpChannel->ack($tag);

            return null;
        }

        try {
            $message = $this->codec->decode($fields);
        } catch (SerializationException $serializationException) {
            $this->logger->error('Undecodable message {tag} moved to the dead letter queue: {error}', ['tag' => $tag, 'error' => $serializationException->getMessage()]);
            $amqpChannel->publish(new AmqpMessage($amqpMessage->body, [
                ...$amqpMessage->headers,
                self::FAILURE_KIND => FailureKind::Serialization->value,
                self::FAILURE_ERROR_CLASS => SerializationException::class,
                self::FAILURE_MESSAGE => $serializationException->getMessage(),
                self::FAILURE_AT => $now->format('Uv'),
            ], $amqpMessage->messageId), '', $this->topology->deadLetterQueue);
            $this->confirmed($amqpChannel);
            $amqpChannel->ack($tag);

            return null;
        }

        $this->handedOut[$tag] = true;
        $previousDeliveries = (int) ($amqpMessage->headers[self::HEADER_DELIVERY_COUNT] ?? 0);

        return new Delivery($message, $this->connector->generation().':'.$tag, $now, $previousDeliveries + 1);
    }

    /**
     * @param Closure(AmqpChannelInterface): void $publish
     */
    private function republishThenAck(Delivery $delivery, Closure $publish): void
    {
        $tag = $this->ownedTag($delivery);
        if (null === $tag) {
            return;
        }

        $this->withChannel(function (AmqpChannelInterface $amqpChannel) use ($publish, $tag): void {
            $publish($amqpChannel);
            $this->confirmed($amqpChannel);
            $amqpChannel->ack($tag);
            unset($this->handedOut[$tag]);
        });
    }

    /**
     * @param array<string, string> $fields
     */
    private function publishMessage(AmqpChannelInterface $amqpChannel, array $fields, int $delayMs): void
    {
        if ($delayMs > 0) {
            $this->publishDelayed($amqpChannel, $fields, $this->nowMs() + $delayMs);

            return;
        }

        $amqpChannel->publish($this->toAmqp($fields), $this->topology->exchange, $fields[EnvelopeCodec::FIELD_CHANNEL] ?? '');
    }

    /**
     * @param array<string, string> $fields
     */
    private function publishDelayed(AmqpChannelInterface $amqpChannel, array $fields, int $dueAtMs): void
    {
        $tier = $this->retryTiers->tierFor(max(1, $dueAtMs - $this->nowMs()));
        $amqpChannel->publish($this->toAmqp($fields, [self::HEADER_DUE_AT => (string) $dueAtMs]), '', $this->topology->retryQueue($tier));
    }

    /**
     * @throws CommandFailedException se il broker non conferma
     */
    private function confirmed(AmqpChannelInterface $amqpChannel): void
    {
        $failed = $amqpChannel->confirm($this->confirmTimeoutMs);
        if ([] !== $failed) {
            throw CommandFailedException::rejected('basic.publish', \sprintf('message %s was not confirmed by the broker', implode(', ', $failed)));
        }
    }

    /**
     * @return list<AmqpMessage>
     */
    private function takeDeadLetters(AmqpChannelInterface $amqpChannel, int $limit): array
    {
        $taken = [];
        while (\count($taken) < $limit && ($amqpMessage = $amqpChannel->get($this->topology->deadLetterQueue)) instanceof AmqpMessage) {
            $taken[] = $amqpMessage;
        }

        return $taken;
    }

    /**
     * @param list<AmqpMessage> $amqpMessages
     */
    private function returnAll(AmqpChannelInterface $amqpChannel, array $amqpMessages): void
    {
        foreach ($amqpMessages as $amqpMessage) {
            $amqpChannel->nack($this->tag($amqpMessage), requeue: true);
        }
    }

    /**
     * Ripubblica nella coda principale con attempts = 0 e conferma i messaggi prelevati dalla DLQ.
     *
     * @param list<AmqpMessage> $amqpMessages
     */
    private function requeue(AmqpChannelInterface $amqpChannel, array $amqpMessages): int
    {
        if ([] === $amqpMessages) {
            return 0;
        }

        foreach ($amqpMessages as $amqpMessage) {
            $amqpChannel->publish($this->toAmqp([...$this->fields($amqpMessage), EnvelopeCodec::FIELD_ATTEMPTS => '0']), '', $this->topology->queue);
        }

        $this->confirmed($amqpChannel);
        foreach ($amqpMessages as $amqpMessage) {
            $amqpChannel->ack($this->tag($amqpMessage));
        }

        return \count($amqpMessages);
    }

    private function toDeadLetter(AmqpMessage $amqpMessage): ?DeadLetter
    {
        try {
            $message = $this->codec->decode($this->fields($amqpMessage));
        } catch (SerializationException) {
            return null;
        }

        $headers = $amqpMessage->headers;
        $errorClass = (string) ($headers[self::FAILURE_ERROR_CLASS] ?? '');
        $failedAtMs = (int) ($headers[self::FAILURE_AT] ?? 0);

        return new DeadLetter($message, new FailureReason(
            FailureKind::tryFrom((string) ($headers[self::FAILURE_KIND] ?? '')) ?? FailureKind::HandlerFailed,
            $message->id,
            new DateTimeImmutable(\sprintf('@%d.%03d', intdiv($failedAtMs, 1000), $failedAtMs % 1000)),
            (string) ($headers[self::FAILURE_MESSAGE] ?? ''),
            '' === $errorClass ? null : $errorClass,
        ));
    }

    /**
     * Campi del wire format: header `x-qb-<campo>` (esclusi quelli di servizio) + body come `text`.
     *
     * @return array<string, string>
     */
    private function fields(AmqpMessage $amqpMessage): array
    {
        $fields = [];
        foreach ($amqpMessage->headers as $name => $value) {
            if (str_starts_with($name, self::HEADER_PREFIX) && self::HEADER_DUE_AT !== $name && !str_starts_with($name, self::HEADER_PREFIX.'failure-')) {
                $fields[substr($name, \strlen(self::HEADER_PREFIX))] = (string) $value;
            }
        }

        $fields[EnvelopeCodec::FIELD_TEXT] = $amqpMessage->body;

        return $fields;
    }

    /**
     * @param array<string, string>     $fields
     * @param array<string, string|int> $extraHeaders
     */
    private function toAmqp(array $fields, array $extraHeaders = []): AmqpMessage
    {
        $headers = [];
        foreach ($fields as $name => $value) {
            if (EnvelopeCodec::FIELD_TEXT !== $name) {
                $headers[self::HEADER_PREFIX.$name] = $value;
            }
        }

        return new AmqpMessage($fields[EnvelopeCodec::FIELD_TEXT] ?? '', [...$headers, ...$extraHeaders], $fields[EnvelopeCodec::FIELD_ID] ?? null);
    }

    /**
     * @return array<string, string>
     */
    private function failureHeaders(FailureReason $failureReason): array
    {
        return [
            self::FAILURE_KIND => $failureReason->kind->value,
            self::FAILURE_ERROR_CLASS => $failureReason->errorClass ?? '',
            self::FAILURE_MESSAGE => $failureReason->errorMessage,
            self::FAILURE_AT => $failureReason->failedAt->format('Uv'),
        ];
    }

    /**
     * Legge, senza attendere, i messaggi già arrivati dal broker, finché il buffer ne contiene $max o non
     * ne arrivano altri.
     */
    private function drainPrefetched(AmqpChannelInterface $amqpChannel, int $max): void
    {
        while (\count($this->buffer) < $max) {
            $before = \count($this->buffer);
            $amqpChannel->wait(0);
            if (\count($this->buffer) === $before) {
                return;
            }
        }
    }

    private function ensureConsuming(AmqpChannelInterface $amqpChannel): void
    {
        if ($this->consumingGeneration === $this->connector->generation()) {
            return;
        }

        $this->forgetChannel();
        $amqpChannel->prefetch($this->prefetch);
        $amqpChannel->consume($this->topology->queue, function (AmqpMessage $amqpMessage): void {
            $this->buffer[] = $amqpMessage;
        });
        $this->consumingGeneration = $this->connector->generation();
    }

    private function forgetChannel(): void
    {
        $this->buffer = [];
        $this->handedOut = [];
        $this->consumingGeneration = null;
    }

    /**
     * Delivery tag della consegna se è ancora posseduta da questo consumer sul canale corrente.
     */
    private function ownedTag(Delivery $delivery): ?int
    {
        [$generation, $tag] = explode(':', $delivery->receipt, 2) + [1 => ''];
        if ((string) $this->connector->generation() !== $generation || !isset($this->handedOut[(int) $tag])) {
            return null;
        }

        return (int) $tag;
    }

    private function tag(AmqpMessage $amqpMessage): int
    {
        return $amqpMessage->deliveryTag ?? throw CommandFailedException::unexpectedReply('basic.get', 'a delivery tag');
    }

    /**
     * Esegue sul canale corrente; dopo un errore del backend il canale viene abbandonato, così il prossimo
     * uso ne apre uno nuovo (in AMQP un errore chiude sempre il canale).
     *
     * @template T
     *
     * @param Closure(AmqpChannelInterface): T $operation
     *
     * @throws BackendException
     *
     * @return T
     */
    private function withChannel(Closure $operation): mixed
    {
        try {
            return $operation($this->connector->channel());
        } catch (BackendException $backendException) {
            if (!$backendException instanceof BatchSendException) {
                $this->close();
            }

            throw $backendException;
        }
    }

    private function assertDelay(int $delayMs): void
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }
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
}
