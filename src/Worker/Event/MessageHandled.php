<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Message\Delivery;

/**
 * L'handler è terminato con successo e il messaggio è stato confermato.
 *
 * @api
 */
final readonly class MessageHandled implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Delivery $delivery,
        public int $durationMs,
    ) {
    }
}
