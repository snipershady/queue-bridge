<?php

declare(strict_types=1);

namespace QueueBridge\Transport\RabbitMq;

use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\UnsupportedStrategyException;
use QueueBridge\Factory\StrategyDtoInterface;
use QueueBridge\Factory\TransportBuilderInterface;
use QueueBridge\System\SystemClock;
use QueueBridge\Transport\TransportInterface;

/**
 * @api
 */
final readonly class RabbitMqTransportBuilder implements TransportBuilderInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        private ClockInterface $clock = new SystemClock(),
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function supports(StrategyDtoInterface $strategy): bool
    {
        return $strategy instanceof StrategyDtoRabbitMq;
    }

    /**
     * Complessità: O(T), T = scaglioni; nessuna connessione.
     */
    #[Override]
    public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
    {
        if (!$strategy instanceof StrategyDtoRabbitMq) {
            throw UnsupportedStrategyException::noBuilderFor($strategy, [self::class]);
        }

        return new RabbitMqTransport(
            $strategy->connector,
            $config,
            $this->clock,
            $strategy->retryTiers,
            $strategy->prefetch,
            logger: $this->logger,
        );
    }
}
