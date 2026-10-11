<?php

declare(strict_types=1);

namespace QueueBridge\Retry;

use Override;

/**
 * @api
 */
final readonly class NoRetry implements RetryStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function nextDelayMs(int $attempt): ?int
    {
        return null;
    }
}
