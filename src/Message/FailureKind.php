<?php

declare(strict_types=1);

namespace snipershady\Queue\Message;

/**
 * Motivo per cui un messaggio finisce in DLQ. I valori fanno parte del wire format: non rinominarli.
 *
 * @api
 */
enum FailureKind: string
{
    /**
     * L'handler ha fallito oltre maxAttempts.
     */
    case HandlerFailed = 'handler_failed';

    /**
     * L'handler ha lanciato UnrecoverableMessageException.
     */
    case Unrecoverable = 'unrecoverable';

    /**
     * Il messaggio salvato non è decodificabile.
     */
    case Serialization = 'serialization';

    /**
     * Nessun handler registrato per il canale.
     */
    case UnknownChannel = 'unknown_channel';

    /**
     * Consegnato oltre maxDeliveries senza conferma (il worker crasha sul messaggio).
     */
    case MaxDeliveriesExceeded = 'max_deliveries_exceeded';

    /**
     * Scaduto prima di essere processato.
     */
    case Expired = 'expired';
}
