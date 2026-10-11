<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

use QueueBridge\Message\Delivery;

/**
 * @api
 */
final readonly class MessageRetried implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Delivery $delivery,
        public int $delayMs,
    ) {
    }
}
