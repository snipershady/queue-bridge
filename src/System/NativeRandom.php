<?php

declare(strict_types=1);

namespace QueueBridge\System;

use InvalidArgumentException;
use Override;

/**
 * @api
 */
final readonly class NativeRandom implements RandomInterface
{
    /**
     * Complessità: O(l), l = numero di byte.
     */
    #[Override]
    public function bytes(int $length): string
    {
        if ($length < 1) {
            throw new InvalidArgumentException(\sprintf('Random length must be >= 1, got %d.', $length));
        }

        return random_bytes($length);
    }
}
