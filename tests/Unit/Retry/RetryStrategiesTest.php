<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Retry;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Retry\ExponentialBackoff;
use QueueBridge\Retry\FixedDelay;
use QueueBridge\Retry\NoRetry;
use QueueBridge\System\NativeRandom;
use QueueBridge\Tests\Support\FakeRandom;

#[CoversClass(NoRetry::class)]
#[CoversClass(FixedDelay::class)]
#[CoversClass(ExponentialBackoff::class)]
#[UsesClass(NativeRandom::class)]
final class RetryStrategiesTest extends TestCase
{
    public function testNoRetry(): void
    {
        self::assertNull((new NoRetry())->nextDelayMs(1));
    }

    public function testFixedDelay(): void
    {
        $fixedDelay = new FixedDelay(250);

        self::assertSame(250, $fixedDelay->nextDelayMs(1));
        self::assertSame(250, $fixedDelay->nextDelayMs(10));
        self::assertSame(1000, (new FixedDelay())->nextDelayMs(1));
        self::assertSame(0, (new FixedDelay(0))->nextDelayMs(1));
    }

    public function testFixedDelayRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new FixedDelay(-1);
    }

    public function testExponentialBackoffWithoutJitterDoublesUpToTheMaximum(): void
    {
        $exponentialBackoff = new ExponentialBackoff(initialMs: 100, multiplier: 2.0, maxMs: 1000, jitter: 0.0);

        self::assertSame(
            [100, 100, 200, 400, 800, 1000, 1000],
            array_map($exponentialBackoff->nextDelayMs(...), [0, 1, 2, 3, 4, 5, 100]),
        );
    }

    public function testExponentialBackoffDefaults(): void
    {
        $exponentialBackoff = new ExponentialBackoff(jitter: 0.0);

        self::assertSame(1000, $exponentialBackoff->nextDelayMs(1));
        self::assertSame(2000, $exponentialBackoff->nextDelayMs(2));
        self::assertSame(300_000, $exponentialBackoff->nextDelayMs(50));
        self::assertSame(300_000, $exponentialBackoff->nextDelayMs(\PHP_INT_MAX));
    }

    /**
     * @return iterable<string, array{int, int}>
     */
    public static function jitterBytes(): iterable
    {
        yield 'minimum random gives -jitter' => [0x00, 900];
        yield 'maximum random gives +jitter' => [0xFF, 1100];
    }

    #[DataProvider('jitterBytes')]
    public function testExponentialBackoffAppliesSymmetricJitter(int $byte, int $expected): void
    {
        $exponentialBackoff = new ExponentialBackoff(initialMs: 1000, maxMs: 10_000, jitter: 0.1, random: new FakeRandom($byte));

        self::assertSame($expected, $exponentialBackoff->nextDelayMs(1));
    }

    public function testExponentialBackoffJitterNeverExceedsTheMaximum(): void
    {
        $exponentialBackoff = new ExponentialBackoff(initialMs: 1000, maxMs: 1000, jitter: 1.0, random: new FakeRandom(0xFF));

        self::assertSame(1000, $exponentialBackoff->nextDelayMs(1));
        self::assertSame(0, (new ExponentialBackoff(initialMs: 1000, jitter: 1.0, random: new FakeRandom(0x00)))->nextDelayMs(1));
    }

    public function testExponentialBackoffDefaultRandomStaysWithinJitterBounds(): void
    {
        $exponentialBackoff = new ExponentialBackoff(initialMs: 1000, jitter: 0.5);

        for ($i = 0; $i < 50; ++$i) {
            $delay = $exponentialBackoff->nextDelayMs(1);
            self::assertGreaterThanOrEqual(500, $delay);
            self::assertLessThanOrEqual(1500, $delay);
        }
    }

    /**
     * @return iterable<string, array{array<string, float|int>, string}>
     */
    public static function invalidBackoffs(): iterable
    {
        yield 'negative initial' => [['initialMs' => -1], 'Initial delay must be >= 0'];
        yield 'multiplier below one' => [['multiplier' => 0.9], 'Multiplier must be >= 1'];
        yield 'max below initial' => [['initialMs' => 1000, 'maxMs' => 999], 'Max delay must be >= initial delay'];
        yield 'negative jitter' => [['jitter' => -0.1], 'Jitter must be between 0 and 1'];
        yield 'jitter above one' => [['jitter' => 1.1], 'Jitter must be between 0 and 1'];
    }

    /**
     * @param array<string, float|int> $arguments
     */
    #[DataProvider('invalidBackoffs')]
    public function testExponentialBackoffRejectsInvalidParameters(array $arguments, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        /* @phpstan-ignore argument.type (named arguments da data provider: i tipi sono verificati dal costruttore) */
        new ExponentialBackoff(...$arguments);
    }

    public function testExponentialBackoffAcceptsBoundaryParameters(): void
    {
        $exponentialBackoff = new ExponentialBackoff(initialMs: 0, multiplier: 1.0, maxMs: 0, jitter: 1.0, random: new FakeRandom(0xFF));

        self::assertSame(0, $exponentialBackoff->nextDelayMs(3));
        self::assertSame(500, (new ExponentialBackoff(initialMs: 500, multiplier: 1.0, maxMs: 500, jitter: 0.0))->nextDelayMs(9));
    }
}
