<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\System;

use DateTimeImmutable;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\System\NativeMemoryMeter;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\NativeSleeper;
use QueueBridge\System\SystemClock;

#[CoversClass(SystemClock::class)]
#[CoversClass(NativeSleeper::class)]
#[CoversClass(NativeRandom::class)]
#[CoversClass(NativeMemoryMeter::class)]
final class NativeSystemTest extends TestCase
{
    public function testSystemClockReturnsCurrentUtcTime(): void
    {
        $before = new DateTimeImmutable();
        $now = (new SystemClock())->now();
        $after = new DateTimeImmutable();

        self::assertSame('UTC', $now->getTimezone()->getName());
        self::assertGreaterThanOrEqual($before, $now);
        self::assertLessThanOrEqual($after, $now);
    }

    public function testSleeperWaitsAtLeastTheRequestedTime(): void
    {
        $start = hrtime(as_number: true);
        (new NativeSleeper())->sleepMs(2);

        self::assertGreaterThanOrEqual(2_000_000, hrtime(as_number: true) - $start);
    }

    public function testSleeperAcceptsZero(): void
    {
        $start = hrtime(as_number: true);
        (new NativeSleeper())->sleepMs(0);

        self::assertLessThan(1_000_000_000, hrtime(as_number: true) - $start);
    }

    public function testSleeperRejectsNegativeDuration(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new NativeSleeper())->sleepMs(-1);
    }

    public function testRandomReturnsRequestedLength(): void
    {
        $nativeRandom = new NativeRandom();

        self::assertSame(16, \strlen($nativeRandom->bytes(16)));
        self::assertSame(1, \strlen($nativeRandom->bytes(1)));
        self::assertNotSame($nativeRandom->bytes(16), $nativeRandom->bytes(16));
    }

    public function testRandomRejectsNonPositiveLength(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new NativeRandom())->bytes(0);
    }

    public function testMemoryMeter(): void
    {
        $nativeMemoryMeter = new NativeMemoryMeter();

        self::assertGreaterThan(0, $nativeMemoryMeter->usageBytes());
        self::assertGreaterThanOrEqual(0, $nativeMemoryMeter->collectCycles());
    }
}
