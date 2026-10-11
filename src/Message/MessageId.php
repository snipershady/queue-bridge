<?php

declare(strict_types=1);

namespace snipershady\Queue\Message;

use InvalidArgumentException;
use Override;
use Stringable;

/**
 * Identificativo applicativo di un messaggio: UUID (RFC 9562) in forma canonica minuscola.
 * La libreria genera UUIDv7; in lettura accetta ogni versione RFC, così il formato resta aperto a evoluzioni.
 *
 * @api
 */
final readonly class MessageId implements Stringable
{
    private const string PATTERN = '/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/D';

    private function __construct(public string $value)
    {
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function __toString(): string
    {
        return $this->value;
    }

    /**
     * Complessità: O(1).
     *
     * @throws InvalidArgumentException se il valore non è un UUID RFC 9562
     */
    public static function fromString(string $value): self
    {
        $normalized = strtolower($value);
        if (1 !== \preg_match(self::PATTERN, $normalized)) {
            throw new InvalidArgumentException(\sprintf('Invalid message id "%s": expected an RFC 9562 UUID.', $value));
        }

        return new self($normalized);
    }

    /**
     * Complessità: O(1).
     *
     * @throws InvalidArgumentException se i byte non sono 16 o non formano un UUID RFC 9562
     */
    public static function fromBytes(string $bytes): self
    {
        if (16 !== \strlen($bytes)) {
            throw new InvalidArgumentException(\sprintf('Invalid message id: expected 16 bytes, got %d.', \strlen($bytes)));
        }

        $hex = bin2hex($bytes);

        return self::fromString(\sprintf(
            '%s-%s-%s-%s-%s',
            \substr($hex, 0, 8),
            \substr($hex, 8, 4),
            \substr($hex, 12, 4),
            \substr($hex, 16, 4),
            \substr($hex, 20, 12),
        ));
    }

    /**
     * Complessità: O(1).
     */
    public function equals(self $other): bool
    {
        return $this->value === $other->value;
    }
}
