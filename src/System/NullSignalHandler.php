<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use Override;

/**
 * Usato quando `ext-pcntl` non è disponibile: nessun graceful shutdown sui segnali.
 *
 * @api
 */
final readonly class NullSignalHandler implements SignalHandlerInterface
{
    #[Override]
    public function onTerminate(callable $onTerminate): void
    {
    }

    #[Override]
    public function reset(): void
    {
    }
}
