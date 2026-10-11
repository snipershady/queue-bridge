<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;
use snipershady\Queue\System\MemoryMeterInterface;

/**
 * Limiti di `WorkerConfig` che fanno uscire `run()`.
 *
 * @internal
 */
final readonly class StopConditions
{
    public function __construct(
        private WorkerConfig $config,
        private ClockInterface $clock,
        private MemoryMeterInterface $memoryMeter,
    ) {
    }

    public function check(int $processed, DateTimeImmutable $startedAt): ?WorkerExitReason
    {
        if (null !== $this->config->maxMessages && $processed >= $this->config->maxMessages) {
            return WorkerExitReason::MessageLimitReached;
        }

        if (null !== $this->config->maxRuntimeSeconds
            && $this->clock->now()->getTimestamp() - $startedAt->getTimestamp() >= $this->config->maxRuntimeSeconds) {
            return WorkerExitReason::TimeLimitReached;
        }

        if (null !== $this->config->memoryLimitMb
            && $this->memoryMeter->usageBytes() >= $this->config->memoryLimitMb * 1024 * 1024) {
            return WorkerExitReason::MemoryLimitReached;
        }

        return null;
    }

    /**
     * Quanti messaggi chiedere al prossimo `receive()` senza superare `maxMessages`.
     */
    public function nextBatchSize(int $processed): int
    {
        if (null === $this->config->maxMessages) {
            return $this->config->batchSize;
        }

        return \max(1, \min($this->config->batchSize, $this->config->maxMessages - $processed));
    }
}
