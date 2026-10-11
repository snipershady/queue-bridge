<?php

declare(strict_types=1);

namespace QueueBridge\Transport;

use DateTimeImmutable;
use InvalidArgumentException;
use QueueBridge\Message\Message;

/**
 * Messaggio ancora in coda, letto senza consumarlo.
 *
 * @api
 */
final readonly class QueuedMessage
{
    /**
     * Complessità: O(1).
     *
     * @param ?DateTimeImmutable $availableAt quando il messaggio diventa disponibile: solo per i differiti
     *
     * @throws InvalidArgumentException se $availableAt manca per un differito o è presente per gli altri stati
     */
    public function __construct(
        public Message $message,
        public QueuedMessageState $state,
        public ?DateTimeImmutable $availableAt = null,
    ) {
        if ((QueuedMessageState::Delayed === $state) !== ($availableAt instanceof DateTimeImmutable)) {
            throw new InvalidArgumentException(\sprintf('availableAt is required for delayed messages only, got state "%s".', $state->value));
        }
    }
}
