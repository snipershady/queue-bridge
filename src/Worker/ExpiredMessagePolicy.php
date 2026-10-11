<?php

declare(strict_types=1);

namespace QueueBridge\Worker;

/**
 * Cosa fare di un messaggio ricevuto dopo la sua scadenza (`Message::$expiresAt`).
 *
 * @api
 */
enum ExpiredMessagePolicy: string
{
    case Discard = 'discard';
    case DeadLetter = 'dead_letter';
}
