<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use InvalidArgumentException;
use Override;

/**
 * @api
 */
final readonly class NativeSleeper implements SleeperInterface
{
    #[Override]
    public function sleepMs(int $milliseconds): void
    {
        if ($milliseconds < 0) {
            throw new InvalidArgumentException(\sprintf('Sleep duration must be >= 0 ms, got %d.', $milliseconds));
        }

        usleep($milliseconds * 1000);
    }
}
