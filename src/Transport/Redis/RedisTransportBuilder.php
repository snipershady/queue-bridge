<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis;

use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Factory\TransportBuilderInterface;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\RandomInterface;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Transport\Redis\Feature\FeatureSelector;
use snipershady\Queue\Transport\Redis\Version\RedisCapabilities;
use snipershady\Queue\Transport\Redis\Version\VersionDetector;
use snipershady\Queue\Transport\TransportInterface;

/**
 * Rileva (o verifica) la versione del server una volta, sceglie le strategie e crea il transport.
 *
 * @api
 */
final readonly class RedisTransportBuilder implements TransportBuilderInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        private ClockInterface $clock = new SystemClock(),
        private LoggerInterface $logger = new NullLogger(),
        private RandomInterface $random = new NativeRandom(),
    ) {
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function supports(StrategyDtoInterface $strategy): bool
    {
        return $strategy instanceof StrategyDtoRedis;
    }

    /**
     * Complessità: O(1), un round trip (INFO server) salvo livello forzato senza verifica.
     */
    #[Override]
    public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
    {
        if (!$strategy instanceof StrategyDtoRedis) {
            throw UnsupportedStrategyException::noBuilderFor($strategy, [self::class]);
        }

        $level = (new VersionDetector($strategy->client, $this->logger))->resolve($strategy->version, $strategy->verifyVersion);
        $this->logger->info('Redis transport for queue {queue} uses level {level}.', ['queue' => $config->name, 'level' => $level->value]);

        return new RedisTransport(
            $strategy->client,
            FeatureSelector::select(RedisCapabilities::forVersion($level)),
            $strategy->consumer ?? 'consumer-'.bin2hex($this->random->bytes(8)),
            $config,
            $this->clock,
            logger: $this->logger,
        );
    }
}
