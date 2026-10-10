<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use InvalidArgumentException;
use snipershady\Queue\Middleware\MiddlewareInterface;
use snipershady\Queue\Retry\ExponentialBackoff;
use snipershady\Queue\Retry\RetryStrategyInterface;
use snipershady\Queue\Worker\Event\WorkerEventListenerInterface;

/**
 * Comportamento del worker. I limiti a null non vengono applicati.
 *
 * @api
 */
final readonly class WorkerConfig
{
    /**
     * @param int                                $batchSize             messaggi chiesti a ogni `receive()`
     * @param int                                $receiveTimeoutMs      attesa massima di `receive()` a coda vuota
     * @param ?int                               $maxMessages           esce dopo aver elaborato questi messaggi
     * @param ?int                               $maxRuntimeSeconds     esce dopo questo tempo
     * @param ?int                               $memoryLimitMb         esce oltre questa memoria
     * @param bool                               $stopWhenEmpty         esce alla prima ricezione vuota
     * @param int                                $maintenanceIntervalMs intervallo di `maintain()` sui transport che la richiedono
     * @param int                                $gcEveryMessages       `gc_collect_cycles()` ogni N messaggi; 0 = mai
     * @param bool                               $retryAnyThrowable     ritenta ogni errore, non solo RetryableException
     * @param list<MiddlewareInterface>          $middlewares           eseguiti nell'ordine dato, attorno all'handler
     * @param list<WorkerEventListenerInterface> $listeners
     *
     * @throws InvalidArgumentException se un valore è fuori dai limiti
     */
    public function __construct(
        public int $batchSize = 10,
        public int $receiveTimeoutMs = 1000,
        public ?int $maxMessages = null,
        public ?int $maxRuntimeSeconds = null,
        public ?int $memoryLimitMb = null,
        public bool $stopWhenEmpty = false,
        public int $maintenanceIntervalMs = 5000,
        public int $gcEveryMessages = 1000,
        public RetryStrategyInterface $retryStrategy = new ExponentialBackoff(),
        public bool $retryAnyThrowable = false,
        public UnknownChannelPolicy $unknownChannelPolicy = UnknownChannelPolicy::DeadLetter,
        public ExpiredMessagePolicy $expiredMessagePolicy = ExpiredMessagePolicy::Discard,
        public array $middlewares = [],
        public array $listeners = [],
    ) {
        $this->assertAtLeast('batchSize', $batchSize, 1);
        $this->assertAtLeast('receiveTimeoutMs', $receiveTimeoutMs, 0);
        $this->assertAtLeast('maxMessages', $maxMessages, 1);
        $this->assertAtLeast('maxRuntimeSeconds', $maxRuntimeSeconds, 1);
        $this->assertAtLeast('memoryLimitMb', $memoryLimitMb, 1);
        $this->assertAtLeast('maintenanceIntervalMs', $maintenanceIntervalMs, 0);
        $this->assertAtLeast('gcEveryMessages', $gcEveryMessages, 0);
    }

    private function assertAtLeast(string $field, ?int $value, int $minimum): void
    {
        if (null !== $value && $value < $minimum) {
            throw new InvalidArgumentException(\sprintf('%s must be >= %d, got %d.', $field, $minimum, $value));
        }
    }
}
