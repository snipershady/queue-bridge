<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\InMemory;

use Override;
use Psr\Clock\ClockInterface;
use snipershady\Queue\Factory\StrategyDtoInterface;

/**
 * Coda in memoria, nel processo corrente: per test e sviluppo.
 *
 * @api
 */
final readonly class StrategyDtoInMemory implements StrategyDtoInterface
{
    /**
     * Complessità: O(1).
     *
     * @param ?ClockInterface $clock orologio della coda (es. finto nei test); null = quello del factory
     */
    public function __construct(public ?ClockInterface $clock = null)
    {
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function name(): string
    {
        return 'in-memory';
    }
}
