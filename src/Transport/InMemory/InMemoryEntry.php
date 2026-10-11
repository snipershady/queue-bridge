<?php

declare(strict_types=1);

namespace QueueBridge\Transport\InMemory;

use QueueBridge\Message\Message;

/**
 * Copia di un messaggio nella coda in memoria, con il numero di consegne già fatte.
 *
 * @internal
 */
final readonly class InMemoryEntry
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public Message $message,
        public int $deliveries = 0,
    ) {
    }

    /**
     * Complessità: O(1).
     */
    public function delivered(): self
    {
        return new self($this->message, $this->deliveries + 1);
    }
}
