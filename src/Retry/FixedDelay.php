<?php

declare(strict_types=1);

namespace snipershady\Queue\Retry;

use InvalidArgumentException;
use Override;

/**
 * @api
 */
final readonly class FixedDelay implements RetryStrategyInterface
{
    /**
     * @throws InvalidArgumentException se $delayMs < 0
     */
    public function __construct(private int $delayMs = 1000)
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }
    }

    #[Override]
    public function nextDelayMs(int $attempt): int
    {
        return $this->delayMs;
    }
}
