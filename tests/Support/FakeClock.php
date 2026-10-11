<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use DateTimeImmutable;
use DateTimeZone;
use Override;
use Psr\Clock\ClockInterface;

final class FakeClock implements ClockInterface
{
    private DateTimeImmutable $now;

    public function __construct(string $now = '2026-01-01T00:00:00.000Z')
    {
        $this->now = (new DateTimeImmutable($now))->setTimezone(new DateTimeZone('UTC'));
    }

    #[Override]
    public function now(): DateTimeImmutable
    {
        return $this->now;
    }

    public function advanceMs(int $milliseconds): void
    {
        $this->now = $this->now->modify(\sprintf('%+d milliseconds', $milliseconds));
    }
}
