<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Codec;

use DateTimeImmutable;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Codec\EnvelopeCodec;
use snipershady\Queue\Exception\SerializationException;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;

#[CoversClass(EnvelopeCodec::class)]
#[UsesClass(SerializationException::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
final class EnvelopeCodecTest extends TestCase
{
    private const string ID = '0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    public function testEncodesVersion1(): void
    {
        $message = new Message(
            MessageId::fromString(self::ID),
            'test',
            '{"a":1} è',
            new DateTimeImmutable('2026-01-01T00:00:00.123Z'),
            2,
            new DateTimeImmutable('2026-01-01T00:01:00.456Z'),
            ['correlation_id' => 'c/1', 'note' => 'è'],
        );

        self::assertSame([
            'v' => '1',
            'id' => self::ID,
            'channel' => 'test',
            'text' => '{"a":1} è',
            'attempts' => '2',
            'created_at' => '1767225600123',
            'headers' => '{"correlation_id":"c/1","note":"è"}',
            'expires_at' => '1767225660456',
        ], (new EnvelopeCodec())->encode($message));
    }

    public function testEncodesEmptyHeadersAsObjectAndOmitsMissingExpiration(): void
    {
        $fields = (new EnvelopeCodec())->encode(
            new Message(MessageId::fromString(self::ID), 'test', '', new DateTimeImmutable('2026-01-01T00:00:00Z')),
        );

        self::assertSame('{}', $fields['headers']);
        self::assertArrayNotHasKey('expires_at', $fields);
    }

    public function testRoundTrip(): void
    {
        $envelopeCodec = new EnvelopeCodec();
        $message = new Message(
            MessageId::fromString(self::ID),
            'orders.created',
            "free text\nwith newline",
            new DateTimeImmutable('2026-01-01T00:00:00.007Z'),
            5,
            new DateTimeImmutable('2026-01-02T00:00:00Z'),
            ['h' => 'v', 'correlation_id' => 'c-1', 'note' => 'è'],
        );

        $decoded = $envelopeCodec->decode($envelopeCodec->encode($message));

        self::assertTrue($decoded->id->equals($message->id));
        self::assertSame($message->channel, $decoded->channel);
        self::assertSame($message->text, $decoded->text);
        self::assertSame(5, $decoded->attempts);
        self::assertSame('2026-01-01T00:00:00.007+00:00', $decoded->createdAt->format('Y-m-d\TH:i:s.vP'));
        self::assertSame('2026-01-02T00:00:00.000+00:00', $decoded->expiresAt?->format('Y-m-d\TH:i:s.vP'));
        self::assertSame(['h' => 'v', 'correlation_id' => 'c-1', 'note' => 'è'], $decoded->headers);
    }

    public function testDecodeIgnoresUnknownFieldsAndOptionalExpiration(): void
    {
        $fields = $this->validFields();
        unset($fields['expires_at']);
        $fields['added_in_a_future_version'] = 'x';

        $message = (new EnvelopeCodec())->decode($fields);

        self::assertNull($message->expiresAt);
        self::assertSame([], $message->headers);
    }

    /**
     * @return iterable<string, array{array<string, string>, string}>
     */
    public static function invalidFields(): iterable
    {
        $valid = [
            'v' => '1',
            'id' => self::ID,
            'channel' => 'test',
            'text' => 'text',
            'attempts' => '0',
            'created_at' => '1767225600000',
            'headers' => '{}',
        ];

        $without = static function (string $field) use ($valid): array {
            unset($valid[$field]);

            return $valid;
        };

        yield 'missing version' => [$without('v'), 'Invalid wire format field "v": missing.'];
        yield 'unknown version' => [['v' => '2'] + $valid, 'Unsupported wire format version "2".'];
        yield 'missing id' => [$without('id'), 'field "id": missing.'];
        yield 'missing channel' => [$without('channel'), 'field "channel": missing.'];
        yield 'missing text' => [$without('text'), 'field "text": missing.'];
        yield 'missing attempts' => [$without('attempts'), 'field "attempts": missing.'];
        yield 'missing created_at' => [$without('created_at'), 'field "created_at": missing.'];
        yield 'missing headers' => [$without('headers'), 'field "headers": missing.'];
        yield 'negative attempts' => [['attempts' => '-1'] + $valid, 'field "attempts": expected a non-negative integer'];
        yield 'attempts with leading zero' => [['attempts' => '01'] + $valid, 'field "attempts"'];
        yield 'attempts overflowing' => [['attempts' => '99999999999999999999'] + $valid, 'field "attempts"'];
        yield 'non numeric created_at' => [['created_at' => 'yesterday'] + $valid, 'field "created_at"'];
        yield 'non numeric expires_at' => [['expires_at' => '1.5'] + $valid, 'field "expires_at"'];
        yield 'invalid headers json' => [['headers' => '{'] + $valid, 'field "headers"'];
        yield 'headers not an object' => [['headers' => '"x"'] + $valid, 'field "headers": expected a JSON object.'];
        yield 'nested headers' => [['headers' => '{"a":{"b":"c"}}'] + $valid, 'field "headers": Maximum stack depth exceeded'];
        yield 'non string header value' => [['headers' => '{"a":1}'] + $valid, 'expected string names and values.'];
        yield 'numeric header name' => [['headers' => '{"1":"v"}'] + $valid, 'expected string names and values.'];
        yield 'invalid header name' => [['headers' => '{"a b":"v"}'] + $valid, 'field "message": Invalid header name'];
        yield 'invalid id' => [['id' => 'nope'] + $valid, 'field "message": Invalid message id'];
        yield 'invalid channel' => [['channel' => 'a b'] + $valid, 'field "message": Invalid channel'];
        yield 'invalid utf-8 text' => [['text' => "\xFF"] + $valid, 'field "message": Invalid text'];
        yield 'expiration before creation' => [['expires_at' => '1'] + $valid, 'field "message": Expiration'];
    }

    /**
     * @param array<string, string> $fields
     */
    #[DataProvider('invalidFields')]
    public function testDecodeRejectsInvalidInput(array $fields, string $error): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage($error);

        (new EnvelopeCodec())->decode($fields);
    }

    /**
     * @return array<string, string>
     */
    private function validFields(): array
    {
        return [
            'v' => '1',
            'id' => self::ID,
            'channel' => 'test',
            'text' => 'text',
            'attempts' => '0',
            'created_at' => '1767225600000',
            'expires_at' => '1767225660000',
            'headers' => '{}',
        ];
    }
}
