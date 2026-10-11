<?php

declare(strict_types=1);

namespace QueueBridge\Message;

use InvalidArgumentException;

/**
 * Messaggio pronto per il transport, con il proprio ritardo (input di `sendBatch`).
 *
 * @api
 */
final readonly class Envelope
{
    /**
     * Complessità: O(1).
     *
     * @throws InvalidArgumentException se $delayMs < 0
     */
    public function __construct(
        public Message $message,
        public int $delayMs = 0,
    ) {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }
    }
}
