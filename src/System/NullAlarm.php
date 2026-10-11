<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use InvalidArgumentException;
use Override;

/**
 * Usato quando `ext-pcntl` non è disponibile: non interrompe nulla (`isSupported()` = false).
 *
 * @api
 */
final readonly class NullAlarm implements AlarmInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function isSupported(): bool
    {
        return false;
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
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function cancel(): void
    {
    }
}
