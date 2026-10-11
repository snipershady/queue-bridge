<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

use QueueBridge\Message\Delivery;

/**
 * Messaggio ricevuto ma restituito alla coda senza elaborarlo (graceful shutdown).
 *
 * @api
 */
final readonly class MessageReleased implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(public Delivery $delivery)
    {
    }
}
