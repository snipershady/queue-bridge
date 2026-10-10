<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use Throwable;

/**
 * Il backend ha rifiutato un comando o ha risposto in un formato inatteso.
 *
 * @api
 */
final class CommandFailedException extends BackendException
{
    public static function rejected(string $command, string $error, ?Throwable $previous = null): self
    {
        return new self(\sprintf('%s failed: %s', $command, $error), 0, $previous);
    }

    public static function unexpectedReply(string $context, string $expected): self
    {
        return new self(\sprintf('Unexpected reply for %s: expected %s.', $context, $expected));
    }
}
