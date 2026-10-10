<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\InMemory;

use snipershady\Queue\Message\Message;

/**
 * Copia di un messaggio nella coda in memoria, con il numero di consegne già fatte.
 *
 * @internal
 */
final readonly class InMemoryEntry
{
    public function __construct(
        public Message $message,
        public int $deliveries = 0,
    ) {
    }

    public function delivered(): self
    {
        return new self($this->message, $this->deliveries + 1);
    }
}
