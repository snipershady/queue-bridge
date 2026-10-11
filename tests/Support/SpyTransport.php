<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use Override;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Transport\Capability\BatchAckAwareInterface;
use QueueBridge\Transport\Capability\DeadLetterAwareInterface;
use QueueBridge\Transport\Capability\MaintenanceAwareInterface;
use QueueBridge\Transport\Capability\StatsAwareInterface;
use QueueBridge\Transport\InMemory\InMemoryTransport;
use QueueBridge\Transport\MaintenanceReport;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;

/**
 * Decoratore di InMemoryTransport che registra le chiamate a maintain(), ack() e ackBatch().
 */
final class SpyTransport implements TransportInterface, MaintenanceAwareInterface, BatchAckAwareInterface, DeadLetterAwareInterface, StatsAwareInterface
{
    public int $maintenances = 0;

    public int $acks = 0;

    /** @var list<int> dimensione di ogni ackBatch() */
    public array $ackBatches = [];

    public function __construct(
        public readonly InMemoryTransport $inner,
        private readonly bool $batchAck = true,
    ) {
    }

    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        $this->inner->send($message, $delayMs);
    }

    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        $this->inner->sendBatch($envelopes);
    }

    #[Override]
    public function receive(int $max = 1, int $timeoutMs = 1000): array
    {
        return $this->inner->receive($max, $timeoutMs);
    }

    #[Override]
    public function ack(Delivery $delivery): void
    {
        ++$this->acks;
        $this->inner->ack($delivery);
    }

    #[Override]
    public function ackBatch(array $deliveries): void
    {
        $this->ackBatches[] = \count($deliveries);
        $this->inner->ackBatch($deliveries);
    }

    #[Override]
    public function retry(Delivery $delivery, int $delayMs): void
    {
        $this->inner->retry($delivery, $delayMs);
    }

    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $this->inner->reject($delivery, $reason);
    }

    #[Override]
    public function release(Delivery $delivery): void
    {
        $this->inner->release($delivery);
    }

    #[Override]
    public function ping(): void
    {
        $this->inner->ping();
    }

    #[Override]
    public function capabilities(): TransportCapabilities
    {
        $capabilities = $this->inner->capabilities();

        return new TransportCapabilities(
            orphanRecovery: $capabilities->orphanRecovery,
            nativeDelay: $capabilities->nativeDelay,
            batchReceive: $capabilities->batchReceive,
            batchAck: $this->batchAck,
            deadLetter: $capabilities->deadLetter,
            consumerLag: $capabilities->consumerLag,
            deliveryCount: $capabilities->deliveryCount,
        );
    }

    #[Override]
    public function maintain(): MaintenanceReport
    {
        ++$this->maintenances;

        return $this->inner->maintain();
    }

    #[Override]
    public function stats(): QueueStats
    {
        return $this->inner->stats();
    }

    #[Override]
    public function deadLetters(int $limit = 100): array
    {
        return $this->inner->deadLetters($limit);
    }

    #[Override]
    public function requeueDeadLetters(int $limit = 100): int
    {
        return $this->inner->requeueDeadLetters($limit);
    }

    #[Override]
    public function requeueDeadLetter(MessageId $id): bool
    {
        return $this->inner->requeueDeadLetter($id);
    }

    #[Override]
    public function purgeDeadLetters(): int
    {
        return $this->inner->purgeDeadLetters();
    }
}
