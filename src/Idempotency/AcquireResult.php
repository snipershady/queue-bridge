<?php

declare(strict_types=1);

namespace QueueBridge\Idempotency;

use InvalidArgumentException;

/**
 * Esito di `IdempotencyStoreInterface::acquire()`: lock acquisito (con il token), messaggio in elaborazione
 * altrove (con il tempo residuo del lock) oppure già completato.
 *
 * @api
 */
final readonly class AcquireResult
{
    /**
     * @param ?LockToken $token           presente solo se `Acquired`
     * @param int        $lockRemainingMs tempo residuo del lock altrui se `InProgress` (>= 1), altrimenti 0
     */
    private function __construct(
        public AcquireStatus $status,
        public ?LockToken $token = null,
        public int $lockRemainingMs = 0,
    ) {
    }

    /**
     * Complessità: O(1).
     */
    public static function acquired(LockToken $token): self
    {
        return new self(AcquireStatus::Acquired, $token);
    }

    /**
     * Complessità: O(1).
     *
     * @param int $lockRemainingMs tempo residuo del lock del worker che sta elaborando il messaggio
     *
     * @throws InvalidArgumentException se $lockRemainingMs < 1
     */
    public static function inProgress(int $lockRemainingMs): self
    {
        if ($lockRemainingMs < 1) {
            throw new InvalidArgumentException(\sprintf('Remaining lock time must be >= 1 ms, got %d.', $lockRemainingMs));
        }

        return new self(AcquireStatus::InProgress, lockRemainingMs: $lockRemainingMs);
    }

    /**
     * Complessità: O(1).
     */
    public static function completed(): self
    {
        return new self(AcquireStatus::Completed);
    }
}
