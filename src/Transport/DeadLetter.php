<?php

declare(strict_types=1);

namespace QueueBridge\Transport;

use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;

/**
 * Messaggio in DLQ con il motivo del fallimento.
 *
 * @api
 */
final readonly class DeadLetter
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Message $message,
        public FailureReason $reason,
    ) {
    }
}
