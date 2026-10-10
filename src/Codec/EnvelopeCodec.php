<?php

declare(strict_types=1);

namespace snipershady\Queue\Codec;

use DateTimeImmutable;
use InvalidArgumentException;
use JsonException;
use Override;
use snipershady\Queue\Exception\SerializationException;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;

/**
 * Wire format versionato. Versione 1:
 *
 * | campo        | contenuto                                   |
 * |--------------|---------------------------------------------|
 * | `v`          | `"1"`                                       |
 * | `id`         | UUID canonico                               |
 * | `channel`    | canale                                      |
 * | `text`       | payload grezzo, mai ricodificato            |
 * | `attempts`   | intero >= 0                                 |
 * | `created_at` | Unix epoch in millisecondi                  |
 * | `expires_at` | Unix epoch in millisecondi (opzionale)      |
 * | `headers`    | oggetto JSON `{"nome": "valore"}`           |
 *
 * I campi sconosciuti vengono ignorati, così una versione futura può aggiungerne senza rompere i lettori.
 *
 * @api
 */
final readonly class EnvelopeCodec implements EnvelopeCodecInterface
{
    public const string VERSION = '1';

    public const string FIELD_VERSION = 'v';

    public const string FIELD_ID = 'id';

    public const string FIELD_CHANNEL = 'channel';

    public const string FIELD_TEXT = 'text';

    public const string FIELD_ATTEMPTS = 'attempts';

    public const string FIELD_CREATED_AT = 'created_at';

    public const string FIELD_EXPIRES_AT = 'expires_at';

    public const string FIELD_HEADERS = 'headers';

    private const string NON_NEGATIVE_INT_PATTERN = '/^(0|[1-9]\d{0,17})$/D';

    #[Override]
    public function encode(Message $message): array
    {
        $fields = [
            self::FIELD_VERSION => self::VERSION,
            self::FIELD_ID => $message->id->value,
            self::FIELD_CHANNEL => $message->channel,
            self::FIELD_TEXT => $message->text,
            self::FIELD_ATTEMPTS => (string) $message->attempts,
            self::FIELD_CREATED_AT => $message->createdAt->format('Uv'),
            self::FIELD_HEADERS => json_encode($message->headers, \JSON_THROW_ON_ERROR | \JSON_FORCE_OBJECT | \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES),
        ];

        if ($message->expiresAt instanceof DateTimeImmutable) {
            $fields[self::FIELD_EXPIRES_AT] = $message->expiresAt->format('Uv');
        }

        return $fields;
    }

    #[Override]
    public function decode(array $fields): Message
    {
        $version = $this->field($fields, self::FIELD_VERSION);

        return match ($version) {
            '1' => $this->decodeV1($fields),
            default => throw SerializationException::unsupportedVersion($version),
        };
    }

    /**
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException
     */
    private function decodeV1(array $fields): Message
    {
        $expiresAt = \array_key_exists(self::FIELD_EXPIRES_AT, $fields)
            ? $this->timestamp($fields, self::FIELD_EXPIRES_AT)
            : null;

        try {
            return new Message(
                MessageId::fromString($this->field($fields, self::FIELD_ID)),
                $this->field($fields, self::FIELD_CHANNEL),
                $this->field($fields, self::FIELD_TEXT),
                $this->timestamp($fields, self::FIELD_CREATED_AT),
                $this->nonNegativeInt($fields, self::FIELD_ATTEMPTS),
                $expiresAt,
                $this->headers($fields),
            );
        } catch (InvalidArgumentException $invalidArgumentException) {
            throw SerializationException::invalidField('message', $invalidArgumentException->getMessage(), $invalidArgumentException);
        }
    }

    /**
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException
     */
    private function field(array $fields, string $name): string
    {
        return $fields[$name] ?? throw SerializationException::invalidField($name, 'missing.');
    }

    /**
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException
     */
    private function nonNegativeInt(array $fields, string $name): int
    {
        $value = $this->field($fields, $name);
        if (1 !== preg_match(self::NON_NEGATIVE_INT_PATTERN, $value)) {
            throw SerializationException::invalidField($name, \sprintf('expected a non-negative integer, got "%s".', $value));
        }

        return (int) $value;
    }

    /**
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException
     */
    private function timestamp(array $fields, string $name): DateTimeImmutable
    {
        $milliseconds = $this->nonNegativeInt($fields, $name);

        return new DateTimeImmutable(\sprintf('@%d.%03d', intdiv($milliseconds, 1000), $milliseconds % 1000));
    }

    /**
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException
     *
     * @return array<string, string>
     */
    private function headers(array $fields): array
    {
        try {
            $decoded = json_decode($this->field($fields, self::FIELD_HEADERS), associative: true, depth: 2, flags: \JSON_THROW_ON_ERROR);
        } catch (JsonException $jsonException) {
            throw SerializationException::invalidField(self::FIELD_HEADERS, $jsonException->getMessage(), $jsonException);
        }

        if (!\is_array($decoded)) {
            throw SerializationException::invalidField(self::FIELD_HEADERS, 'expected a JSON object.');
        }

        $headers = [];
        foreach ($decoded as $name => $value) {
            if (!\is_string($name) || !\is_string($value)) {
                throw SerializationException::invalidField(self::FIELD_HEADERS, 'expected string names and values.');
            }

            $headers[$name] = $value;
        }

        return $headers;
    }
}
