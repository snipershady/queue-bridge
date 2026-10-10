<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

/**
 * Cosa fare di un messaggio il cui canale non ha handler (né fallback).
 *
 * @api
 */
enum UnknownChannelPolicy: string
{
    case DeadLetter = 'dead_letter';
    case Discard = 'discard';
}
