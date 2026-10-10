<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use InvalidArgumentException;
use Override;

/**
 * Allarme basato su SIGALRM (`ext-pcntl`), con risoluzione al secondo.
 *
 * @api
 */
final readonly class PcntlAlarm implements AlarmInterface
{
    #[Override]
    public function isSupported(): bool
    {
        return true;
    }

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

    #[Override]
    public function cancel(): void
    {
        pcntl_alarm(0);
        pcntl_signal(\SIGALRM, \SIG_DFL);
    }
}
