<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\InMemory;

use Override;
use Psr\Clock\ClockInterface;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Factory\TransportBuilderInterface;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Transport\TransportInterface;

/**
 * @api
 */
final readonly class InMemoryTransportBuilder implements TransportBuilderInterface
{
    public function __construct(private ClockInterface $clock = new SystemClock())
    {
    }

    #[Override]
    public function supports(StrategyDtoInterface $strategy): bool
    {
        return $strategy instanceof StrategyDtoInMemory;
    }

    #[Override]
    public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
    {
        if (!$strategy instanceof StrategyDtoInMemory) {
            throw UnsupportedStrategyException::noBuilderFor($strategy, [self::class]);
        }

        return new InMemoryTransport($strategy->clock ?? $this->clock, $config);
    }
}
