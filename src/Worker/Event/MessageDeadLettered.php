<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureReason;

/**
 * @api
 */
final readonly class MessageDeadLettered implements WorkerEventInterface
{
    public function __construct(
        public Delivery $delivery,
        public FailureReason $reason,
    ) {
    }
}
