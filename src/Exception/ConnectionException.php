<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use Throwable;

/**
 * Backend irraggiungibile anche dopo i tentativi di riconnessione.
 *
 * @api
 */
final class ConnectionException extends BackendException
{
    /**
     * Complessità: O(1).
     */
    public static function unreachable(string $backend, string $reason, ?Throwable $previous = null): self
    {
        return new self(\sprintf('%s backend is unreachable: %s', $backend, $reason), 0, $previous);
    }
}
