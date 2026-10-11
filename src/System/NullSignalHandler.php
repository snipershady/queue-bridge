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
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function onTerminate(callable $onTerminate): void
    {
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function reset(): void
    {
    }
}
