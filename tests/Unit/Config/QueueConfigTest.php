<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Config;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Config\QueueConfig;

#[CoversClass(QueueConfig::class)]
final class QueueConfigTest extends TestCase
{
    public function testDefaults(): void
    {
        $queueConfig = new QueueConfig();

        self::assertSame('default', $queueConfig->name);
        self::assertSame('rq', $queueConfig->prefix);
        self::assertSame(1_048_576, $queueConfig->maxTextBytes);
        self::assertSame(65_536, $queueConfig->maxHeadersBytes);
        self::assertSame(3, $queueConfig->maxAttempts);
        self::assertSame(10, $queueConfig->maxDeliveries);
        self::assertSame(300_000, $queueConfig->claimIdleMs);
        self::assertSame(100_000, $queueConfig->dlqMaxLength);
    }

    public function testAcceptsBoundaryValues(): void
    {
        $queueConfig = new QueueConfig(
            name: str_repeat('a', 100),
            prefix: 'My_prefix-1',
            maxTextBytes: 1,
            maxHeadersBytes: 0,
            maxAttempts: 1,
            maxDeliveries: 2,
            claimIdleMs: 1000,
            dlqMaxLength: 1,
        );

        self::assertSame(2, $queueConfig->maxDeliveries);
        self::assertSame(0, $queueConfig->maxHeadersBytes);
    }

    /**
     * @return iterable<string, array{array<string, int|string>, string}>
     */
    public static function invalidConfigs(): iterable
    {
        yield 'empty name' => [['name' => ''], 'Invalid name ""'];
        yield 'name with colon' => [['name' => 'a:b'], 'Invalid name'];
        yield 'name with dot' => [['name' => 'a.b'], 'Invalid name'];
        yield 'name with hash tag' => [['name' => '{a}'], 'Invalid name'];
        yield 'name too long' => [['name' => str_repeat('a', 101)], 'Invalid name'];
        yield 'invalid prefix' => [['prefix' => 'r q'], 'Invalid prefix'];
        yield 'zero text bytes' => [['maxTextBytes' => 0], 'maxTextBytes must be >= 1, got 0.'];
        yield 'negative header bytes' => [['maxHeadersBytes' => -1], 'maxHeadersBytes must be >= 0'];
        yield 'zero attempts' => [['maxAttempts' => 0], 'maxAttempts must be >= 1'];
        yield 'deliveries not above attempts' => [['maxAttempts' => 5, 'maxDeliveries' => 5], 'maxDeliveries must be >= 6, got 5.'];
        yield 'claim idle too short' => [['claimIdleMs' => 999], 'claimIdleMs must be >= 1000'];
        yield 'zero dlq length' => [['dlqMaxLength' => 0], 'dlqMaxLength must be >= 1'];
    }

    /**
     * @param array<string, int|string> $arguments
     */
    #[DataProvider('invalidConfigs')]
    public function testRejectsInvalidValues(array $arguments, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        /* @phpstan-ignore argument.type (named arguments da data provider: i tipi sono verificati dal costruttore) */
        new QueueConfig(...$arguments);
    }
}
