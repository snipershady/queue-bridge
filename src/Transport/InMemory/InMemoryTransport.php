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
 * Complessità dei metodi: t = byte di `text`, h = byte degli header, P = consegne pendenti, d = differiti,
 * T = scaglioni di attesa (RabbitMQ). "Round trip" = scambi di rete con il backend.
 *
 * @api
 */
final readonly class InMemoryTransport implements TransportInterface, SetupAwareInterface, MaintenanceAwareInterface, StatsAwareInterface, DeadLetterAwareInterface, BatchAckAwareInterface, PurgeAwareInterface, BrowseAwareInterface
{
    private InMemoryBroker $inMemoryBroker;

    /**
     * Complessità: O(1).
     */
    public function __construct(
        private ClockInterface $clock,
        private QueueConfig $config = new QueueConfig(),
    ) {
        $this->inMemoryBroker = new InMemoryBroker();
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function setup(): void
    {
    }

    /**
     * Un altro consumer della stessa coda (stesso stato condiviso), come un secondo worker.
     *
     * Complessità: O(1).
     */
    public function newConsumer(): self
    {
        return clone $this;
    }

    /**
     * Complessità: O(1) ammortizzato.
     */
    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }

        $nowMs = $this->nowMs();
        $this->inMemoryBroker->enqueue(new InMemoryEntry($message), $nowMs + $delayMs, $nowMs);
    }

    /**
     * Complessità: O(n), n = messaggi.
     */
    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        foreach ($envelopes as $envelope) {
            $this->send($envelope->message, $envelope->delayMs);
        }
    }

    /**
     * Complessità: O(d log d + k·r), d = differiti, k = consegne restituite, r = messaggi pronti.
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

        $now = $this->clock->now();
        $nowMs = (int) $now->format('Uv');
        $this->inMemoryBroker->promoteDue($nowMs);

        return array_map(
            static fn (array $taken): Delivery => new Delivery($taken['entry']->message, $taken['receipt'], $now, $taken['entry']->deliveries),
            $this->inMemoryBroker->take($max, $nowMs),
        );
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function ack(Delivery $delivery): void
    {
        $this->inMemoryBroker->settle($delivery->receipt);
    }

    /**
     * Complessità: O(n), n = consegne.
     */
    #[Override]
    public function ackBatch(array $deliveries): void
    {
        foreach ($deliveries as $delivery) {
            $this->ack($delivery);
        }
    }

    /**
     * Complessità: O(t + h), t = byte del testo, h = byte degli header (validazione UTF-8 e dei nomi).
     */
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

    /**
     * Complessità: O(1) ammortizzato; O(l) quando la DLQ supera la lunghezza massima, l = messaggi in DLQ.
     */
    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $inMemoryEntry = $this->inMemoryBroker->settle($delivery->receipt);
        if (!$inMemoryEntry instanceof InMemoryEntry) {
            return;
        }

        $this->inMemoryBroker->addDeadLetter(new DeadLetter($inMemoryEntry->message, $reason), $this->config->dlqMaxLength);
    }

    /**
     * Complessità: O(r), r = messaggi pronti.
     */
    #[Override]
    public function release(Delivery $delivery): void
    {
        $inMemoryEntry = $this->inMemoryBroker->settle($delivery->receipt);
        if ($inMemoryEntry instanceof InMemoryEntry) {
            $this->inMemoryBroker->pushFront($inMemoryEntry);
        }
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function ping(): void
    {
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
            consumerLag: true,
            deliveryCount: true,
        );
    }

    /**
     * Complessità: O(d log d + f), d = differiti, f = messaggi in elaborazione.
     */
    #[Override]
    public function maintain(): MaintenanceReport
    {
        $nowMs = $this->nowMs();

        return new MaintenanceReport(
            promotedDelayed: $this->inMemoryBroker->promoteDue($nowMs),
            recoveredOrphans: $this->inMemoryBroker->recoverOrphans($nowMs, $this->config->claimIdleMs),
        );
    }

    /**
     * Complessità: O(d log d + r), d = differiti, r = messaggi pronti.
     */
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

    /**
     * Complessità: O(limit).
     */
    #[Override]
    public function deadLetters(int $limit = 100): array
    {
        return $this->inMemoryBroker->deadLetters($this->assertLimit($limit));
    }

    /**
     * Complessità: O(l + limit·(t + h)), l = messaggi in DLQ.
     */
    #[Override]
    public function requeueDeadLetters(int $limit = 100): int
    {
        $deadLetters = $this->inMemoryBroker->takeDeadLetters($this->assertLimit($limit));
        foreach ($deadLetters as $deadLetter) {
            $this->requeue($deadLetter);
        }

        return \count($deadLetters);
    }

    /**
     * Complessità: O(l + t + h), l = messaggi in DLQ.
     */
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

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function purgeDeadLetters(): int
    {
        return $this->inMemoryBroker->purgeDeadLetters();
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function purge(): int
    {
        return $this->inMemoryBroker->purge();
    }

    /**
     * Complessità: O(f + r + d log d), f = in elaborazione, r = pronti, d = differiti.
     */
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
