<?php

declare(strict_types=1);

namespace snipershady\Queue\Message;

use InvalidArgumentException;

/**
 * Invarianti condivise da Message e OutgoingMessage. I limiti di dimensione configurabili
 * (QueueConfig) sono applicati dal producer, non qui.
 *
 * @internal
 */
final class MessageRules
{
    public const int MAX_CHANNEL_LENGTH = 255;

    public const int MAX_HEADER_NAME_LENGTH = 255;

    private const string CHANNEL_PATTERN = '/^[A-Za-z0-9._:-]+$/D';

    private const string HEADER_NAME_PATTERN = '/^[A-Za-z][A-Za-z0-9._-]*$/D';

    /**
     * @throws InvalidArgumentException
     */
    public static function assertChannel(string $channel): void
    {
        if (\strlen($channel) > self::MAX_CHANNEL_LENGTH || 1 !== preg_match(self::CHANNEL_PATTERN, $channel)) {
            throw new InvalidArgumentException(\sprintf('Invalid channel "%s": expected 1-%d characters matching %s.', $channel, self::MAX_CHANNEL_LENGTH, self::CHANNEL_PATTERN));
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function assertText(string $text): void
    {
        if (!self::isValidUtf8($text)) {
            throw new InvalidArgumentException('Invalid text: expected valid UTF-8.');
        }
    }

    /**
     * @param array<string, string> $headers
     *
     * @throws InvalidArgumentException
     */
    public static function assertHeaders(array $headers): void
    {
        foreach ($headers as $name => $value) {
            if (!\is_string($name)
                || \strlen($name) > self::MAX_HEADER_NAME_LENGTH
                || 1 !== preg_match(self::HEADER_NAME_PATTERN, $name)) {
                throw new InvalidArgumentException(\sprintf('Invalid header name "%s": expected 1-%d characters matching %s.', $name, self::MAX_HEADER_NAME_LENGTH, self::HEADER_NAME_PATTERN));
            }

            if (!\is_string($value) || !self::isValidUtf8($value)) {
                throw new InvalidArgumentException(\sprintf('Invalid value for header "%s": expected a UTF-8 string.', $name));
            }
        }
    }

    public static function isValidUtf8(string $value): bool
    {
        return 1 === preg_match('//u', $value);
    }

    /**
     * Tronca a $maxBytes byte senza spezzare un carattere UTF-8.
     */
    public static function truncateUtf8(string $value, int $maxBytes): string
    {
        $truncated = substr($value, 0, max(0, $maxBytes));
        while (!self::isValidUtf8($truncated)) {
            $truncated = substr($truncated, 0, -1);
        }

        return $truncated;
    }
}
