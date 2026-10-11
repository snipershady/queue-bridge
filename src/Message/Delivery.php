<?php

declare(strict_types=1);

namespace snipershady\Queue\Message;

use DateTimeImmutable;
use InvalidArgumentException;

/**
 * Messaggio ricevuto da un transport.
 *
 * @api
 */
final readonly class Delivery
{
    /**
     * Complessità: O(1).
     *
     * @param string $receipt       identificativo del backend (ID dello stream, delivery tag...): opaco per il core
     * @param int    $deliveryCount consegne di questa copia del messaggio, crash del worker inclusi (>= 1);
     *                              riparte da 1 dopo un retry, che crea una nuova copia
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public Message $message,
        public string $receipt,
        public DateTimeImmutable $receivedAt,
        public int $deliveryCount = 1,
    ) {
        if ('' === $receipt) {
            throw new InvalidArgumentException('Delivery receipt must not be empty.');
        }

        if ($deliveryCount < 1) {
            throw new InvalidArgumentException(\sprintf('Delivery count must be >= 1, got %d.', $deliveryCount));
        }
    }
}
