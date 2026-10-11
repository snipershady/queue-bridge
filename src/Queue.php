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

    #[Override]
    public function setup(): void
    {
        if ($this->transport instanceof SetupAwareInterface) {
            $this->transport->setup();
        }
    }

    #[Override]
    public function producer(): ProducerInterface
    {
        return $this->producer ??= new Producer($this->transport, $this->config, $this->clock, middlewares: $this->producerMiddlewares);
    }

    #[Override]
    public function worker(WorkerConfig $config = new WorkerConfig()): WorkerInterface
    {
        return new Worker($this->transport, $this->config, $config, $this->clock, $this->signalHandler, $this->memoryMeter, $this->logger);
    }

    #[Override]
    public function transport(): TransportInterface
    {
        return $this->transport;
    }

    #[Override]
    public function ping(): void
    {
        $this->transport->ping();
    }

    #[Override]
    public function stats(): ?QueueStats
    {
        return $this->transport instanceof StatsAwareInterface ? $this->transport->stats() : null;
    }

    #[Override]
    public function deadLetters(): ?DeadLetterAwareInterface
    {
        return $this->transport instanceof DeadLetterAwareInterface ? $this->transport : null;
    }

    #[Override]
    public function purge(): ?int
    {
        return $this->transport instanceof PurgeAwareInterface ? $this->transport->purge() : null;
    }

    #[Override]
    public function browse(int $limit = 100): ?array
    {
        return $this->transport instanceof BrowseAwareInterface ? $this->transport->browse($limit) : null;
    }

    #[Override]
    public function capabilities(): TransportCapabilities
    {
        return $this->transport->capabilities();
    }
}
