<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq;

use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Factory\TransportBuilderInterface;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Transport\TransportInterface;

/**
 * @api
 */
final readonly class RabbitMqTransportBuilder implements TransportBuilderInterface
{
    public function __construct(
        private ClockInterface $clock = new SystemClock(),
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    #[Override]
    public function supports(StrategyDtoInterface $strategy): bool
    {
        return $strategy instanceof StrategyDtoRabbitMq;
    }

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
