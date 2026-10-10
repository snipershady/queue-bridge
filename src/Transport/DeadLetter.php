<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;

/**
 * Messaggio in DLQ con il motivo del fallimento.
 *
 * @api
 */
final readonly class DeadLetter
{
    public function __construct(
        public Message $message,
        public FailureReason $reason,
    ) {
    }
}
