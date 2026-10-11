<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Support;

use InvalidArgumentException;
use Override;
use snipershady\Queue\System\RandomInterface;

/**
 * Restituisce byte deterministici: i chunk configurati in ordine (troncati o completati alla lunghezza
 * richiesta), poi il byte di riempimento ripetuto.
 */
final class FakeRandom implements RandomInterface
{
    /**
     * @param list<string> $chunks
     */
    public function __construct(private readonly int $byte = 0x00, private array $chunks = [])
    {
    }

    #[Override]
    public function bytes(int $length): string
    {
        if ($length < 1) {
            throw new InvalidArgumentException('Random length must be >= 1.');
        }

        $chunk = array_shift($this->chunks) ?? '';

        return \substr($chunk.str_repeat(\chr($this->byte), $length), 0, $length);
    }
}
