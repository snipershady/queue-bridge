<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use snipershady\Queue\Factory\StrategyDtoInterface;

/**
 * Nessun builder registrato nel QueueFactory accetta il DTO ricevuto.
 *
 * @api
 */
final class UnsupportedStrategyException extends QueueException
{
    /**
     * @param list<string> $builders classi dei builder registrati
     */
    public static function noBuilderFor(StrategyDtoInterface $strategyDto, array $builders): self
    {
        return new self(\sprintf(
            'No transport builder supports the "%s" strategy (%s). Registered builders: %s.',
            $strategyDto->name(),
            $strategyDto::class,
            [] === $builders ? 'none' : \implode(', ', $builders),
        ));
    }
}
