<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Worker\WorkerExitReason;

/**
 * @api
 */
final readonly class WorkerStopped implements WorkerEventInterface
{
    public function __construct(
        public WorkerExitReason $reason,
        public int $processed,
    ) {
    }
}
