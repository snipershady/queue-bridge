<?php

declare(strict_types=1);

namespace snipershady\Queue\Text;

use InvalidArgumentException;

/**
 * Decodifica sicura del testo: JSON se valido, altrimenti testo grezzo. Non lancia mai eccezioni sul contenuto.
 *
 * @api
 */
final readonly class TextDecoder
{
    public const int DEFAULT_MAX_DEPTH = 512;

    /** Limite imposto da ext-json alla profondità. */
    private const int MAX_SUPPORTED_DEPTH = 2_147_483_647;

    /** @var int<1, 2147483647> */
    private int $maxDepth;

    /**
     * @throws InvalidArgumentException se $maxDepth è fuori da 1..2147483647
     */
    public function __construct(int $maxDepth = self::DEFAULT_MAX_DEPTH)
    {
        if ($maxDepth < 1 || $maxDepth > self::MAX_SUPPORTED_DEPTH) {
            throw new InvalidArgumentException(\sprintf('Max depth must be between 1 and %d, got %d.', self::MAX_SUPPORTED_DEPTH, $maxDepth));
        }

        $this->maxDepth = $maxDepth;
    }

    /**
     * JSON oltre la profondità massima viene trattato come testo semplice.
     */
    public function decode(string $text): DecodedText
    {
        if (!json_validate($text, $this->maxDepth)) {
            return DecodedText::plain($text);
        }

        // json_validate() con la stessa profondità garantisce che la decodifica non fallisca.
        $value = json_decode($text, associative: true, depth: $this->maxDepth, flags: \JSON_THROW_ON_ERROR | \JSON_BIGINT_AS_STRING);
        \assert(\is_array($value) || \is_scalar($value) || null === $value);

        return DecodedText::json($text, $value);
    }
}
