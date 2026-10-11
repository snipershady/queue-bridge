<?php

declare(strict_types=1);

namespace snipershady\Queue\Retry;

use InvalidArgumentException;
use Override;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\RandomInterface;

/**
 * Ritardo `initialMs * multiplier^(attempt - 1)`, limitato a `maxMs`, con jitter simmetrico
 * (±`jitter` × ritardo) per evitare che i worker ritentino tutti nello stesso istante.
 *
 * @api
 */
final readonly class ExponentialBackoff implements RetryStrategyInterface
{
    private const int RANDOM_MAX = 0xFFFFFFFF;

    /**
     * @param float $jitter frazione del ritardo, tra 0 (nessun jitter) e 1
     *
     * @throws InvalidArgumentException se un parametro è fuori dai limiti
     */
    public function __construct(
        private int $initialMs = 1000,
        private float $multiplier = 2.0,
        private int $maxMs = 300_000,
        private float $jitter = 0.1,
        private RandomInterface $random = new NativeRandom(),
    ) {
        if ($initialMs < 0) {
            throw new InvalidArgumentException(\sprintf('Initial delay must be >= 0 ms, got %d.', $initialMs));
        }

        if ($multiplier < 1.0) {
            throw new InvalidArgumentException(\sprintf('Multiplier must be >= 1, got %s.', $multiplier));
        }

        if ($maxMs < $initialMs) {
            throw new InvalidArgumentException(\sprintf('Max delay must be >= initial delay (%d ms), got %d.', $initialMs, $maxMs));
        }

        if ($jitter < 0.0 || $jitter > 1.0) {
            throw new InvalidArgumentException(\sprintf('Jitter must be between 0 and 1, got %s.', $jitter));
        }
    }

    #[Override]
    public function nextDelayMs(int $attempt): int
    {
        $base = \min((float) $this->maxMs, $this->initialMs * $this->multiplier ** \max(0, $attempt - 1));

        if ($this->jitter > 0.0) {
            // Frazione uniforme in [-1, 1] da 32 bit casuali.
            $unit = (int) hexdec(bin2hex($this->random->bytes(4))) / self::RANDOM_MAX * 2 - 1;
            $base += $base * $this->jitter * $unit;
        }

        return (int) round(\min((float) $this->maxMs, \max(0.0, $base)));
    }
}
