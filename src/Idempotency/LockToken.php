<?php

declare(strict_types=1);

namespace QueueBridge\Idempotency;

use InvalidArgumentException;
use QueueBridge\Message\MessageId;

/**
 * Prova di possesso del lock "in elaborazione" di un messaggio.
 *
 * @api
 */
final readonly class LockToken
{
    /**
     * Complessità: O(1).
     *
     * @throws InvalidArgumentException se $token è vuoto
     */
    public function __construct(
        public MessageId $messageId,
        public string $token,
    ) {
        if ('' === $token) {
            throw new InvalidArgumentException('Lock token must not be empty.');
        }
    }
}
