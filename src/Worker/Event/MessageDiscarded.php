<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

use QueueBridge\Message\Delivery;
use QueueBridge\Message\FailureKind;

/**
 * Messaggio confermato senza eseguire l'handler (scaduto, o canale sconosciuto con politica Discard).
 *
 * @api
 */
final readonly class MessageDiscarded implements WorkerEventInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Delivery $delivery,
        public FailureKind $reason,
    ) {
    }
}
