<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use Override;

/**
 * Richiede `ext-pcntl`. I segnali sono asincroni: il callback viene invocato appena il segnale arriva,
 * senza bisogno di chiamare `pcntl_signal_dispatch()`.
 *
 * @api
 */
final readonly class PcntlSignalHandler implements SignalHandlerInterface
{
    private const array SIGNALS = [\SIGTERM, \SIGINT];

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function onTerminate(callable $onTerminate): void
    {
        pcntl_async_signals(enable: true);
        foreach (self::SIGNALS as $signal) {
            pcntl_signal($signal, static function (int $signo) use ($onTerminate): void {
                $onTerminate($signo);
            });
        }
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function reset(): void
    {
        foreach (self::SIGNALS as $signal) {
            pcntl_signal($signal, \SIG_DFL);
        }
    }
}
