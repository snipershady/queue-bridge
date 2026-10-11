<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use InvalidArgumentException;
use Throwable;

/**
 * Lanciata da un handler per un errore transitorio: attiva la retry strategy.
 *
 * @api
 */
final class RetryableException extends QueueException
{
    /**
     * Complessità: O(1).
     *
     * @param ?int $delayMs ritardo del prossimo tentativo; null = quello della retry strategy
     *
     * @throws InvalidArgumentException se $delayMs < 0
     */
    public function __construct(
        string $message = '',
        public readonly ?int $delayMs = null,
        ?Throwable $previous = null,
    ) {
        if (null !== $delayMs && $delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Retry delay must be >= 0 ms, got %d.', $delayMs));
        }

        parent::__construct($message, 0, $previous);
    }
}
