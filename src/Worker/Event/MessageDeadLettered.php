<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

use QueueBridge\Message\Delivery;
use QueueBridge\Message\FailureReason;

/**
 * @api
 */
final readonly class MessageDeadLettered implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Delivery $delivery,
        public FailureReason $reason,
    ) {
    }
}
