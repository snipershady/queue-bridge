<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

/**
 * Stato di un messaggio ancora in coda, restituito da BrowseAwareInterface::browse().
 *
 * @api
 */
enum QueuedMessageState: string
{
    /**
     * Consegnato a un consumer e non ancora confermato.
     */
    case InFlight = 'in_flight';

    /**
     * Disponibile per la prossima ricezione.
     */
    case Ready = 'ready';

    /**
     * Differito: diventa disponibile a `QueuedMessage::$availableAt`.
     */
    case Delayed = 'delayed';
}
