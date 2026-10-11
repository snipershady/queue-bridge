<?php

declare(strict_types=1);

namespace QueueBridge\Transport\InMemory;

use Override;
use Psr\Clock\ClockInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\UnsupportedStrategyException;
use QueueBridge\Factory\StrategyDtoInterface;
use QueueBridge\Factory\TransportBuilderInterface;
use QueueBridge\System\SystemClock;
use QueueBridge\Transport\TransportInterface;

/**
 * @api
 */
final readonly class InMemoryTransportBuilder implements TransportBuilderInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(private ClockInterface $clock = new SystemClock())
    {
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function supports(StrategyDtoInterface $strategy): bool
    {
        return $strategy instanceof StrategyDtoInMemory;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
    {
        if (!$strategy instanceof StrategyDtoInMemory) {
            throw UnsupportedStrategyException::noBuilderFor($strategy, [self::class]);
        }

        return new InMemoryTransport($strategy->clock ?? $this->clock, $config);
    }
}
