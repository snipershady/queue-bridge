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
    /**
     * Complessità: O(1) di calcolo; blocca il processo per $milliseconds.
     */
    #[Override]
    public function sleepMs(int $milliseconds): void
    {
        if ($milliseconds < 0) {
            throw new InvalidArgumentException(\sprintf('Sleep duration must be >= 0 ms, got %d.', $milliseconds));
        }

        usleep($milliseconds * 1000);
    }
}
