<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

/**
 * Nessun handler registrato per il canale del messaggio.
 *
 * @api
 */
final class UnknownChannelException extends QueueException
{
    public static function forChannel(string $channel): self
    {
        return new self(\sprintf('No handler registered for channel "%s".', $channel));
    }
}
