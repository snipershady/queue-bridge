<?php

declare(strict_types=1);

namespace QueueBridgeExamples;

use DateTimeImmutable;
use DateTimeZone;
use Override;
use Psr\Clock\ClockInterface;

/**
 * PSR-20 clock moved by hand: with the InMemory backend it makes delays, retries and expirations instantaneous
 * and deterministic in tests.
 */
final class ManualClock implements ClockInterface
{
    private DateTimeImmutable $now;

    public function __construct(string $start = '2026-01-01T00:00:00Z')
    {
        $this->now = new DateTimeImmutable($start, new DateTimeZone('UTC'));
    }

    #[Override]
    public function now(): DateTimeImmutable
    {
        return $this->now;
    }

    public function advance(int $milliseconds): void
    {
        $this->now = $this->now->modify(\sprintf('+%d milliseconds', $milliseconds));
    }
}
