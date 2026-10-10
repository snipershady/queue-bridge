<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Message\Delivery;

/**
 * Messaggio ricevuto ma restituito alla coda senza elaborarlo (graceful shutdown).
 *
 * @api
 */
final readonly class MessageReleased implements WorkerEventInterface
{
    public function __construct(public Delivery $delivery)
    {
    }
}
