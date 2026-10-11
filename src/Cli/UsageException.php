<?php

declare(strict_types=1);

namespace QueueBridge\Cli;

use InvalidArgumentException;

/**
 * Errore d'uso della CLI (comando o opzione non validi): exit code 2.
 *
 * @internal
 */
final class UsageException extends InvalidArgumentException
{
}
