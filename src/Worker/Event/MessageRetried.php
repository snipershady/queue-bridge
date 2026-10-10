<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Message\Delivery;

/**
 * @api
 */
final readonly class MessageRetried implements WorkerEventInterface
{
    public function __construct(
        public Delivery $delivery,
        public int $delayMs,
    ) {
    }
}
