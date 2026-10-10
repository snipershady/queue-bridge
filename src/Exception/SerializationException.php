<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use Throwable;

/**
 * Messaggio salvato non decodificabile: non recuperabile, va in DLQ senza retry.
 *
 * @api
 */
final class SerializationException extends QueueException
{
    public static function unsupportedVersion(string $version): self
    {
        return new self(\sprintf('Unsupported wire format version "%s".', $version));
    }

    public static function invalidField(string $field, string $reason, ?Throwable $previous = null): self
    {
        return new self(\sprintf('Invalid wire format field "%s": %s', $field, $reason), 0, $previous);
    }
}
