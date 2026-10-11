<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

use QueueBridge\Worker\WorkerExitReason;

/**
 * @api
 */
final readonly class WorkerStopped implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public WorkerExitReason $reason,
        public int $processed,
    ) {
    }
}
