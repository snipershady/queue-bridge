<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

/**
 * La versione del server non supporta il livello richiesto, o è sotto il minimo supportato.
 *
 * @api
 */
final class VersionMismatchException extends BackendException
{
    public static function overrideAboveServer(string $requested, string $server): self
    {
        return new self(\sprintf('Requested Redis level %s but the server reports %s: lower the override or set verifyVersion to false.', $requested, $server));
    }

    public static function unsupported(string $server, string $minimum): self
    {
        return new self(\sprintf('Redis %s is not supported: version %s or later is required.', $server, $minimum));
    }
}
