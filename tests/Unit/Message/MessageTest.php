<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Message;

use DateTimeImmutable;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;

#[CoversClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
final class MessageTest extends TestCase
{
    private const string ID = '0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    public function testExposesItsDataNormalizedToUtc(): void
    {
        $message = new Message(
            MessageId::fromString(self::ID),
            'test',
            '{"a":1}',
            new DateTimeImmutable('2026-01-01T02:00:00+02:00'),
            2,
            new DateTimeImmutable('2026-01-01T03:00:00+02:00'),
            ['correlation_id' => 'c-1'],
        );

        self::assertSame(self::ID, $message->id->value);
        self::assertSame('test', $message->channel);
        self::assertSame('{"a":1}', $message->text);
        self::assertSame(2, $message->attempts);
        self::assertSame(['correlation_id' => 'c-1'], $message->headers);
        self::assertSame('2026-01-01T00:00:00+00:00', $message->createdAt->format(\DATE_ATOM));
        self::assertSame('2026-01-01T01:00:00+00:00', $message->expiresAt?->format(\DATE_ATOM));
    }

    public function testDefaults(): void
    {
        $message = $this->message();

        self::assertSame(0, $message->attempts);
        self::assertNull($message->expiresAt);
        self::assertSame([], $message->headers);
    }

    public function testWithIncrementedAttemptsKeepsEverythingElse(): void
    {
        $message = new Message(
            MessageId::fromString(self::ID),
            'test',
            'text',
            new DateTimeImmutable('2026-01-01T00:00:00Z'),
            0,
            new DateTimeImmutable('2026-01-02T00:00:00Z'),
            ['h' => 'v'],
        );

        $retried = $message->withIncrementedAttempts();

        self::assertSame(0, $message->attempts);
        self::assertSame(1, $retried->attempts);
        self::assertTrue($retried->id->equals($message->id));
        self::assertSame($message->channel, $retried->channel);
        self::assertSame($message->text, $retried->text);
        self::assertSame($message->createdAt->format('Uv'), $retried->createdAt->format('Uv'));
        self::assertSame($message->expiresAt?->format('Uv'), $retried->expiresAt?->format('Uv'));
        self::assertSame($message->headers, $retried->headers);
    }

    public function testWithAttempts(): void
    {
        $message = $this->message()->withAttempts(4);

        self::assertSame(4, $message->attempts);
        self::assertSame(0, $message->withAttempts(0)->attempts);

        $this->expectException(InvalidArgumentException::class);
        $message->withAttempts(-1);
    }

    public function testIsExpired(): void
    {
        $message = new Message(
            MessageId::fromString(self::ID),
            'test',
            'text',
            new DateTimeImmutable('2026-01-01T00:00:00Z'),
            expiresAt: new DateTimeImmutable('2026-01-01T00:00:10Z'),
        );

        self::assertFalse($message->isExpired(new DateTimeImmutable('2026-01-01T00:00:09.999Z')));
        self::assertTrue($message->isExpired(new DateTimeImmutable('2026-01-01T00:00:10Z')));
        self::assertFalse($this->message()->isExpired(new DateTimeImmutable('2100-01-01T00:00:00Z')));
    }

    public function testRejectsNegativeAttempts(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Attempts must be >= 0');

        new Message(MessageId::fromString(self::ID), 'test', 'text', new DateTimeImmutable(), -1);
    }

    public function testRejectsExpirationNotAfterCreation(): void
    {
        $createdAt = new DateTimeImmutable('2026-01-01T00:00:00Z');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expiration must be later than creation');

        new Message(MessageId::fromString(self::ID), 'test', 'text', $createdAt, expiresAt: $createdAt);
    }

    /**
     * @return iterable<string, array{string, string, array<string, string>, string}>
     */
    public static function invalidFields(): iterable
    {
        yield 'channel' => ['bad channel', 'text', [], 'Invalid channel'];
        yield 'text' => ['test', "\xFF", [], 'UTF-8'];
        yield 'header name' => ['test', 'text', ['1a' => 'v'], 'Invalid header name'];
    }

    /**
     * @param array<string, string> $headers
     */
    #[DataProvider('invalidFields')]
    public function testValidatesChannelTextAndHeaders(string $channel, string $text, array $headers, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        new Message(MessageId::fromString(self::ID), $channel, $text, new DateTimeImmutable(), headers: $headers);
    }

    private function message(): Message
    {
        return new Message(MessageId::fromString(self::ID), 'test', 'text', new DateTimeImmutable('2026-01-01T00:00:00Z'));
    }
}
