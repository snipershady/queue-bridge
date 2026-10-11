<?php

declare(strict_types=1);

namespace snipershady\Queue;

use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Producer\Producer;
use snipershady\Queue\Producer\ProducerInterface;
use snipershady\Queue\Producer\ProducerMiddlewareInterface;
use snipershady\Queue\System\MemoryMeterInterface;
use snipershady\Queue\System\SignalHandlerInterface;
use snipershady\Queue\Transport\Capability\BrowseAwareInterface;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
use snipershady\Queue\Transport\Capability\PurgeAwareInterface;
use snipershady\Queue\Transport\Capability\SetupAwareInterface;
use snipershady\Queue\Transport\Capability\StatsAwareInterface;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;
use snipershady\Queue\Worker\Worker;
use snipershady\Queue\Worker\WorkerConfig;
use snipershady\Queue\Worker\WorkerInterface;

/**
 * Facade di una coda: collega transport, producer e worker con la stessa configurazione.
 *
 * @api
 */
final class Queue implements QueueInterface
{
    private ?ProducerInterface $producer = null;

    /**
     * Complessità: O(1).
     *
     * @param list<ProducerMiddlewareInterface> $producerMiddlewares
     */
    public function __construct(
        private readonly TransportInterface $transport,
        private readonly QueueConfig $config,
        private readonly ClockInterface $clock,
        private readonly LoggerInterface $logger,
        private readonly SignalHandlerInterface $signalHandler,
        private readonly MemoryMeterInterface $memoryMeter,
        private readonly array $producerMiddlewares = [],
    ) {
    }

    /**
     * Complessità: Quella di `setup()` del transport: O(1) su Redis e InMemory, O(T) dichiarazioni su RabbitMQ (T =
     * scaglioni).
     */
    #[Override]
    public function setup(): void
    {
        if ($this->transport instanceof SetupAwareInterface) {
            $this->transport->setup();
        }
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function producer(): ProducerInterface
    {
        return $this->producer ??= new Producer($this->transport, $this->config, $this->clock, middlewares: $this->producerMiddlewares);
    }

    /**
     * Complessità: O(m), m = middleware del worker.
     */
    #[Override]
    public function worker(WorkerConfig $config = new WorkerConfig()): WorkerInterface
    {
        return new Worker($this->transport, $this->config, $config, $this->clock, $this->signalHandler, $this->memoryMeter, $this->logger);
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function transport(): TransportInterface
    {
        return $this->transport;
    }

    /**
     * Complessità: O(1), un round trip.
     */
    #[Override]
    public function ping(): void
    {
        $this->transport->ping();
    }

    /**
     * Complessità: Quella di `stats()` del transport: un numero costante di round trip su Redis, O(T) su RabbitMQ.
     */
    #[Override]
    public function stats(): ?QueueStats
    {
        return $this->transport instanceof StatsAwareInterface ? $this->transport->stats() : null;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function deadLetters(): ?DeadLetterAwareInterface
    {
        return $this->transport instanceof DeadLetterAwareInterface ? $this->transport : null;
    }

    /**
     * Complessità: O(n) nei messaggi rimossi (vedi `purge()` del transport).
     */
    #[Override]
    public function purge(): ?int
    {
        return $this->transport instanceof PurgeAwareInterface ? $this->transport->purge() : null;
    }

    /**
     * Complessità: O(limit) messaggi decodificati (vedi `browse()` del transport).
     */
    #[Override]
    public function browse(int $limit = 100): ?array
    {
        return $this->transport instanceof BrowseAwareInterface ? $this->transport->browse($limit) : null;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function capabilities(): TransportCapabilities
    {
        return $this->transport->capabilities();
    }
}
