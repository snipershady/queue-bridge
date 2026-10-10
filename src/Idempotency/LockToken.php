<?php

declare(strict_types=1);

namespace snipershady\Queue\Idempotency;

use InvalidArgumentException;
use snipershady\Queue\Message\MessageId;

/**
 * Prova di possesso del lock "in elaborazione" di un messaggio.
 *
 * @api
 */
final readonly class LockToken
{
    /**
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
