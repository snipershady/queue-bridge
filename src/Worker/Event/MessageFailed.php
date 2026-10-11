<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

use QueueBridge\Message\Delivery;
use Throwable;

/**
 * L'handler (o un middleware) ha lanciato un'eccezione; segue MessageRetried o MessageDeadLettered.
 *
 * @api
 */
final readonly class MessageFailed implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Delivery $delivery,
        public Throwable $error,
    ) {
    }
}
