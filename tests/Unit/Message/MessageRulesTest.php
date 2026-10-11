<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Message;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\DoesNotPerformAssertions;
use PHPUnit\Framework\TestCase;
use QueueBridge\Message\MessageRules;

#[CoversClass(MessageRules::class)]
final class MessageRulesTest extends TestCase
{
    /**
     * @return iterable<string, array{string}>
     */
    public static function validChannels(): iterable
    {
        yield 'simple' => ['test'];
        yield 'all allowed characters' => ['Orders.v2:created_eu-1'];
        yield 'max length' => [str_repeat('a', 255)];
    }

    #[DataProvider('validChannels')]
    #[DoesNotPerformAssertions]
    public function testAcceptsValidChannels(string $channel): void
    {
        MessageRules::assertChannel($channel);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidChannels(): iterable
    {
        yield 'empty' => [''];
        yield 'space' => ['a b'];
        yield 'slash' => ['a/b'];
        yield 'trailing newline' => ["test\n"];
        yield 'too long' => [str_repeat('a', 256)];
    }

    #[DataProvider('invalidChannels')]
    public function testRejectsInvalidChannels(string $channel): void
    {
        $this->expectException(InvalidArgumentException::class);

        MessageRules::assertChannel($channel);
    }

    public function testTextMustBeValidUtf8(): void
    {
        MessageRules::assertText('');
        MessageRules::assertText('plain, {"json":1}, è ✓ 😀');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('UTF-8');

        MessageRules::assertText("\xC3\x28");
    }

    #[DoesNotPerformAssertions]
    public function testAcceptsValidHeaders(): void
    {
        MessageRules::assertHeaders([]);
        MessageRules::assertHeaders(['correlation_id' => 'abc', 'X.Trace-id' => '', 'k' => 'è']);
        MessageRules::assertHeaders([str_repeat('a', 255) => 'max length name']);
    }

    /**
     * @return iterable<string, array{array<mixed>}>
     */
    public static function invalidHeaders(): iterable
    {
        yield 'numeric name' => [[123 => 'v']];
        yield 'name starting with digit' => [['1a' => 'v']];
        yield 'name with space' => [['a b' => 'v']];
        yield 'name too long' => [[str_repeat('a', 256) => 'v']];
        yield 'non string value' => [['a' => 1]];
        yield 'invalid utf-8 value' => [['a' => "\xFF"]];
    }

    /**
     * @param array<mixed> $headers
     */
    #[DataProvider('invalidHeaders')]
    public function testRejectsInvalidHeaders(array $headers): void
    {
        $this->expectException(InvalidArgumentException::class);

        /* @phpstan-ignore argument.type (verifica a runtime di input che violano il tipo dichiarato) */
        MessageRules::assertHeaders($headers);
    }

    public function testTruncateUtf8NeverSplitsACharacter(): void
    {
        self::assertSame('abc', MessageRules::truncateUtf8('abc', 10));
        self::assertSame('ab', MessageRules::truncateUtf8('abc', 2));
        self::assertSame('', MessageRules::truncateUtf8('abc', 0));
        self::assertSame('', MessageRules::truncateUtf8('abc', -5));
        // "😀" occupa 4 byte: tagliare a 5 lascia solo "a".
        self::assertSame('a', MessageRules::truncateUtf8('a😀', 4));
        self::assertSame('a😀', MessageRules::truncateUtf8('a😀b', 5));
    }
}
