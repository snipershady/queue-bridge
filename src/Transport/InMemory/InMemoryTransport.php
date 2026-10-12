<?php

declare(strict_types=1);

namespace QueueBridge\Transport\InMemory;

use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Delivery;
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
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;

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
     * Gli envelope vengono letti tutti prima di accodarli: un errore durante l'iterazione non lascia il batch
     * accodato a metà (come su Redis, che costruisce la pipeline prima di inviarla).
     *
     * Complessità: O(n), n = messaggi.
     */
    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        $batch = [];
        foreach ($envelopes as $envelope) {
            $batch[] = $envelope;
        }

        foreach ($batch as $envelope) {
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
     * Rimette il messaggio in fondo ai pronti, come Redis: l'ordine dopo un release non è garantito su nessun backend.
     *
     * Complessità: O(1) ammortizzato.
     */
    #[Override]
    public function release(Delivery $delivery): void
    {
        $inMemoryEntry = $this->inMemoryBroker->settle($delivery->receipt);
        if ($inMemoryEntry instanceof InMemoryEntry) {
            $this->inMemoryBroker->pushBack($inMemoryEntry);
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
     * Sola lettura, come su Redis: un differito già scaduto resta tra i differiti finché receive() o maintain() non
     * lo promuovono. Il lag sono i messaggi pronti non ancora letti, comprese le copie rimesse in coda da release()
     * o dal recupero degli orfani (su Redis sono entry nuove dello stream).
     *
     * Complessità: O(1).
     */
    #[Override]
    public function stats(): QueueStats
    {
        $ready = $this->inMemoryBroker->readyCount();

        return new QueueStats(
            ready: $ready,
            inFlight: $this->inMemoryBroker->inFlightCount(),
            delayed: $this->inMemoryBroker->delayedCount(),
            deadLetters: $this->inMemoryBroker->deadLetterCount(),
            lag: $ready,
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
