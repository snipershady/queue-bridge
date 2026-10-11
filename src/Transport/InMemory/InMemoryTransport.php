<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\InMemory;

use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Transport\Capability\BatchAckAwareInterface;
use snipershady\Queue\Transport\Capability\BrowseAwareInterface;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
use snipershady\Queue\Transport\Capability\MaintenanceAwareInterface;
use snipershady\Queue\Transport\Capability\PurgeAwareInterface;
use snipershady\Queue\Transport\Capability\SetupAwareInterface;
use snipershady\Queue\Transport\Capability\StatsAwareInterface;
use snipershady\Queue\Transport\DeadLetter;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;

/**
 * Implementazione di riferimento del contratto, in memoria: per test e sviluppo, non per produzione.
 *
 * Il tempo viene da ClockInterface, quindi differiti e orfani si testano senza attese reali. `receive()` non
 * blocca: nello stesso processo nessuno può pubblicare mentre si attende. Gli orfani si recuperano con
 * `maintain()`, come su Redis.
 *
 * @api
 */
final readonly class InMemoryTransport implements TransportInterface, SetupAwareInterface, MaintenanceAwareInterface, StatsAwareInterface, DeadLetterAwareInterface, BatchAckAwareInterface, PurgeAwareInterface, BrowseAwareInterface
{
    private InMemoryBroker $inMemoryBroker;

    public function __construct(
        private ClockInterface $clock,
        private QueueConfig $config = new QueueConfig(),
    ) {
        $this->inMemoryBroker = new InMemoryBroker();
    }

    #[Override]
    public function setup(): void
    {
    }

    /**
     * Un altro consumer della stessa coda (stesso stato condiviso), come un secondo worker.
     */
    public function newConsumer(): self
    {
        return clone $this;
    }

    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }

        $nowMs = $this->nowMs();
        $this->inMemoryBroker->enqueue(new InMemoryEntry($message), $nowMs + $delayMs, $nowMs);
    }

    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        foreach ($envelopes as $envelope) {
            $this->send($envelope->message, $envelope->delayMs);
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

        $now = $this->clock->now();
        $nowMs = (int) $now->format('Uv');
        $this->inMemoryBroker->promoteDue($nowMs);

        return array_map(
            static fn (array $taken): Delivery => new Delivery($taken['entry']->message, $taken['receipt'], $now, $taken['entry']->deliveries),
            $this->inMemoryBroker->take($max, $nowMs),
        );
    }

    #[Override]
    public function ack(Delivery $delivery): void
    {
        $this->inMemoryBroker->settle($delivery->receipt);
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
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }

        $inMemoryEntry = $this->inMemoryBroker->settle($delivery->receipt);
        if (!$inMemoryEntry instanceof InMemoryEntry) {
            return;
        }

        $nowMs = $this->nowMs();
        $this->inMemoryBroker->enqueue(new InMemoryEntry($inMemoryEntry->message->withIncrementedAttempts()), $nowMs + $delayMs, $nowMs);
    }

    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $inMemoryEntry = $this->inMemoryBroker->settle($delivery->receipt);
        if (!$inMemoryEntry instanceof InMemoryEntry) {
            return;
        }

        $this->inMemoryBroker->addDeadLetter(new DeadLetter($inMemoryEntry->message, $reason), $this->config->dlqMaxLength);
    }

    #[Override]
    public function release(Delivery $delivery): void
    {
        $inMemoryEntry = $this->inMemoryBroker->settle($delivery->receipt);
        if ($inMemoryEntry instanceof InMemoryEntry) {
            $this->inMemoryBroker->pushFront($inMemoryEntry);
        }
    }

    #[Override]
    public function ping(): void
    {
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
            consumerLag: true,
            deliveryCount: true,
        );
    }

    #[Override]
    public function maintain(): MaintenanceReport
    {
        $nowMs = $this->nowMs();

        return new MaintenanceReport(
            promotedDelayed: $this->inMemoryBroker->promoteDue($nowMs),
            recoveredOrphans: $this->inMemoryBroker->recoverOrphans($nowMs, $this->config->claimIdleMs),
        );
    }

    #[Override]
    public function stats(): QueueStats
    {
        $this->inMemoryBroker->promoteDue($this->nowMs());

        return new QueueStats(
            ready: $this->inMemoryBroker->readyCount(),
            inFlight: $this->inMemoryBroker->inFlightCount(),
            delayed: $this->inMemoryBroker->delayedCount(),
            deadLetters: $this->inMemoryBroker->deadLetterCount(),
            lag: $this->inMemoryBroker->neverDeliveredCount(),
        );
    }

    #[Override]
    public function deadLetters(int $limit = 100): array
    {
        return $this->inMemoryBroker->deadLetters($this->assertLimit($limit));
    }

    #[Override]
    public function requeueDeadLetters(int $limit = 100): int
    {
        $deadLetters = $this->inMemoryBroker->takeDeadLetters($this->assertLimit($limit));
        foreach ($deadLetters as $deadLetter) {
            $this->requeue($deadLetter);
        }

        return \count($deadLetters);
    }

    #[Override]
    public function requeueDeadLetter(MessageId $id): bool
    {
        $deadLetter = $this->inMemoryBroker->takeDeadLetter($id);
        if (!$deadLetter instanceof DeadLetter) {
            return false;
        }

        $this->requeue($deadLetter);

        return true;
    }

    #[Override]
    public function purgeDeadLetters(): int
    {
        return $this->inMemoryBroker->purgeDeadLetters();
    }

    #[Override]
    public function purge(): int
    {
        return $this->inMemoryBroker->purge();
    }

    #[Override]
    public function browse(int $limit = 100): array
    {
        return $this->inMemoryBroker->browse($this->assertLimit($limit));
    }

    private function requeue(DeadLetter $deadLetter): void
    {
        $nowMs = $this->nowMs();
        $this->inMemoryBroker->enqueue(new InMemoryEntry($deadLetter->message->withAttempts(0)), $nowMs, $nowMs);
    }

    private function assertLimit(int $limit): int
    {
        if ($limit < 1) {
            throw new InvalidArgumentException(\sprintf('Limit must be >= 1, got %d.', $limit));
        }

        return $limit;
    }

    private function nowMs(): int
    {
        return (int) $this->clock->now()->format('Uv');
    }
}
