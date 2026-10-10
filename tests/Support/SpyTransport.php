<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Support;

use Override;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Transport\Capability\BatchAckAwareInterface;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
use snipershady\Queue\Transport\Capability\MaintenanceAwareInterface;
use snipershady\Queue\Transport\Capability\StatsAwareInterface;
use snipershady\Queue\Transport\InMemory\InMemoryTransport;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;

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
