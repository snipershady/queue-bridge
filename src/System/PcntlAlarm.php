<?php

declare(strict_types=1);

namespace QueueBridge\System;

use InvalidArgumentException;
use Override;

/**
 * Allarme basato su SIGALRM (`ext-pcntl`), con risoluzione al secondo.
 *
 * @api
 */
final readonly class PcntlAlarm implements AlarmInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function isSupported(): bool
    {
        return true;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function schedule(int $seconds, callable $onAlarm): void
    {
        if ($seconds < 1) {
            throw new InvalidArgumentException(\sprintf('Alarm delay must be >= 1 second, got %d.', $seconds));
        }

        pcntl_async_signals(enable: true);
        pcntl_signal(\SIGALRM, static function () use ($onAlarm): void {
            $onAlarm();
        });
        pcntl_alarm($seconds);
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function cancel(): void
    {
        pcntl_alarm(0);
        pcntl_signal(\SIGALRM, \SIG_DFL);
    }
}
