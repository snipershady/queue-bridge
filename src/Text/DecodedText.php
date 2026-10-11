<?php

declare(strict_types=1);

namespace snipershady\Queue\Text;

/**
 * Risultato della decodifica di `Message::$text`.
 *
 * - `Json`: `$value` è il JSON decodificato (oggetti come array associativi, anche scalari e null);
 * - `Plain`: `$value` è il testo originale.
 *
 * @api
 */
final readonly class DecodedText
{
    /**
     * @param array<mixed>|string|int|float|bool|null $value
     */
    private function __construct(
        public TextKind $kind,
        public array|string|int|float|bool|null $value,
        public string $raw,
    ) {
    }

    /**
     * Complessità: O(1).
     *
     * @param array<mixed>|string|int|float|bool|null $value
     */
    public static function json(string $raw, array|string|int|float|bool|null $value): self
    {
        return new self(TextKind::Json, $value, $raw);
    }

    /**
     * Complessità: O(1).
     */
    public static function plain(string $raw): self
    {
        return new self(TextKind::Plain, $raw, $raw);
    }

    /**
     * Complessità: O(1).
     */
    public function isJson(): bool
    {
        return TextKind::Json === $this->kind;
    }
}
