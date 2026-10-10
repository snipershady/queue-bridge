<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Message\Delivery;
use Throwable;

/**
 * L'handler (o un middleware) ha lanciato un'eccezione; segue MessageRetried o MessageDeadLettered.
 *
 * @api
 */
final readonly class MessageFailed implements WorkerEventInterface
{
    public function __construct(
        public Delivery $delivery,
        public Throwable $error,
    ) {
    }
}
