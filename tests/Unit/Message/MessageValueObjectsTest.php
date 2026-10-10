<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Message;

use DateTimeImmutable;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\Envelope;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Message\OutgoingMessage;

#[CoversClass(Delivery::class)]
#[CoversClass(Envelope::class)]
#[CoversClass(OutgoingMessage::class)]
#[CoversClass(FailureReason::class)]
#[CoversClass(FailureKind::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
final class MessageValueObjectsTest extends TestCase
{
    public function testDelivery(): void
    {
        $message = $this->message();
        $receivedAt = new DateTimeImmutable('2026-01-01T00:00:01Z');

        $delivery = new Delivery($message, '1700000000000-0', $receivedAt, 3);

        self::assertSame($message, $delivery->message);
        self::assertSame('1700000000000-0', $delivery->receipt);
        self::assertSame($receivedAt, $delivery->receivedAt);
        self::assertSame(3, $delivery->deliveryCount);
        self::assertSame(1, (new Delivery($message, 'r', $receivedAt))->deliveryCount);
    }

    public function testDeliveryRejectsEmptyReceipt(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Delivery($this->message(), '', new DateTimeImmutable());
    }

    public function testDeliveryRejectsNonPositiveDeliveryCount(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Delivery($this->message(), 'r', new DateTimeImmutable(), 0);
    }

    public function testEnvelope(): void
    {
        $message = $this->message();

        self::assertSame(0, (new Envelope($message))->delayMs);
        self::assertSame(1500, (new Envelope($message, 1500))->delayMs);
        self::assertSame($message, (new Envelope($message))->message);

        $this->expectException(InvalidArgumentException::class);
        new Envelope($message, -1);
    }

    public function testOutgoingMessage(): void
    {
        $outgoingMessage = new OutgoingMessage('test', 'text', ['h' => 'v'], 100, 60_000);

        self::assertSame('test', $outgoingMessage->channel);
        self::assertSame('text', $outgoingMessage->text);
        self::assertSame(['h' => 'v'], $outgoingMessage->headers);
        self::assertSame(100, $outgoingMessage->delayMs);
        self::assertSame(60_000, $outgoingMessage->ttlMs);

        self::assertSame(1, (new OutgoingMessage('test', 'text', ttlMs: 1))->ttlMs);

        $defaults = new OutgoingMessage('test', '');
        self::assertSame([], $defaults->headers);
        self::assertSame(0, $defaults->delayMs);
        self::assertNull($defaults->ttlMs);
    }

    /**
     * @return iterable<string, array{string, string, array<string, string>, int, ?int, string}>
     */
    public static function invalidOutgoingMessages(): iterable
    {
        yield 'channel' => ['bad channel', 'text', [], 0, null, 'Invalid channel'];
        yield 'text' => ['test', "\xFF", [], 0, null, 'UTF-8'];
        yield 'header name' => ['test', 'text', ['1a' => 'v'], 0, null, 'Invalid header name'];
        yield 'negative delay' => ['test', 'text', [], -1, null, 'Delay must be >= 0'];
        yield 'non positive ttl' => ['test', 'text', [], 0, 0, 'TTL must be >= 1'];
    }

    /**
     * @param array<string, string> $headers
     */
    #[DataProvider('invalidOutgoingMessages')]
    public function testOutgoingMessageValidation(
        string $channel,
        string $text,
        array $headers,
        int $delayMs,
        ?int $ttlMs,
        string $error,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        new OutgoingMessage($channel, $text, $headers, $delayMs, $ttlMs);
    }

    public function testFailureReasonFromThrowable(): void
    {
        $messageId = MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd');

        $failureReason = FailureReason::fromThrowable(
            FailureKind::HandlerFailed,
            $messageId,
            new DateTimeImmutable('2026-01-01T01:00:00+01:00'),
            new LogicException('boom'),
        );

        self::assertSame(FailureKind::HandlerFailed, $failureReason->kind);
        self::assertSame($messageId, $failureReason->originalId);
        self::assertSame('boom', $failureReason->errorMessage);
        self::assertSame(LogicException::class, $failureReason->errorClass);
        self::assertSame('2026-01-01T00:00:00+00:00', $failureReason->failedAt->format(\DATE_ATOM));
    }

    public function testFailureReasonWithoutException(): void
    {
        $failureReason = new FailureReason(FailureKind::Expired, $this->message()->id, new DateTimeImmutable());

        self::assertSame('', $failureReason->errorMessage);
        self::assertNull($failureReason->errorClass);
    }

    public function testFailureReasonSanitizesAndTruncatesTheErrorMessage(): void
    {
        $id = $this->message()->id;
        $now = new DateTimeImmutable();

        $long = new FailureReason(FailureKind::HandlerFailed, $id, $now, str_repeat('è', 3000));
        self::assertSame(FailureReason::MAX_ERROR_MESSAGE_BYTES, \strlen($long->errorMessage));

        $binary = new FailureReason(FailureKind::HandlerFailed, $id, $now, "bad \xFF\xFE bytes");
        self::assertSame('bad ?? bytes', $binary->errorMessage);
    }

    public function testFailureKindWireValuesAreStable(): void
    {
        self::assertSame(
            ['handler_failed', 'unrecoverable', 'serialization', 'unknown_channel', 'max_deliveries_exceeded', 'expired'],
            array_map(static fn (FailureKind $failureKind): string => $failureKind->value, FailureKind::cases()),
        );
    }

    private function message(): Message
    {
        return new Message(
            MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd'),
            'test',
            'text',
            new DateTimeImmutable('2026-01-01T00:00:00Z'),
        );
    }
}
