<?php

declare(strict_types=1);

namespace snipershady\Queue\Retry;

use Override;

/**
 * @api
 */
final readonly class NoRetry implements RetryStrategyInterface
{
    #[Override]
    public function nextDelayMs(int $attempt): ?int
    {
        return null;
    }
}
