<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Message;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Message\MessageId;

#[CoversClass(MessageId::class)]
final class MessageIdTest extends TestCase
{
    public function testFromStringNormalizesToLowercase(): void
    {
        $messageId = MessageId::fromString('0190F2A4-5B6C-7D8E-9F01-23456789ABCD');

        self::assertSame('0190f2a4-5b6c-7d8e-9f01-23456789abcd', $messageId->value);
        self::assertSame('0190f2a4-5b6c-7d8e-9f01-23456789abcd', (string) $messageId);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidIds(): iterable
    {
        yield 'empty' => [''];
        yield 'not an uuid' => ['not-an-uuid'];
        yield 'version 0' => ['0190f2a4-5b6c-0d8e-9f01-23456789abcd'];
        yield 'version 9' => ['0190f2a4-5b6c-9d8e-9f01-23456789abcd'];
        yield 'wrong variant' => ['0190f2a4-5b6c-7d8e-7f01-23456789abcd'];
        yield 'no dashes' => ['0190f2a45b6c7d8e9f0123456789abcd'];
        yield 'trailing newline' => ["0190f2a4-5b6c-7d8e-9f01-23456789abcd\n"];
    }

    #[DataProvider('invalidIds')]
    public function testFromStringRejectsInvalidValues(string $value): void
    {
        $this->expectException(InvalidArgumentException::class);

        MessageId::fromString($value);
    }

    public function testFromBytesFormatsCanonicalUuid(): void
    {
        $messageId = MessageId::fromBytes((string) hex2bin('0190f2a45b6c7d8e9f0123456789abcd'));

        self::assertSame('0190f2a4-5b6c-7d8e-9f01-23456789abcd', $messageId->value);
    }

    public function testFromBytesRejectsWrongLength(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('expected 16 bytes, got 15');

        MessageId::fromBytes(str_repeat("\x00", 15));
    }

    public function testEquals(): void
    {
        $messageId = MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd');

        self::assertTrue($messageId->equals(MessageId::fromString('0190F2A4-5B6C-7D8E-9F01-23456789ABCD')));
        self::assertFalse($messageId->equals(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abce')));
    }
}
