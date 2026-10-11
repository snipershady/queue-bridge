<?php

declare(strict_types=1);

namespace QueueBridge\Retry;

use InvalidArgumentException;
use Override;

/**
 * @api
 */
final readonly class FixedDelay implements RetryStrategyInterface
{
    /**
     * Complessità: O(1).
     *
     * @throws InvalidArgumentException se $delayMs < 0
     */
    public function __construct(private int $delayMs = 1000)
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function nextDelayMs(int $attempt): int
    {
        return $this->delayMs;
    }
}
