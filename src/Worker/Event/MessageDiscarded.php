<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureKind;

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
