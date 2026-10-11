<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Integration\Redis;

use Override;
use Psr\Clock\ClockInterface;
use Redis;
use RedisException;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Tests\Contract\TransportContractTestCase;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Feature\FeatureSelector;
use snipershady\Queue\Transport\Redis\RedisTransport;
use snipershady\Queue\Transport\Redis\Version\RedisCapabilities;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;
use snipershady\Queue\Transport\Redis\Version\VersionDetector;
use snipershady\Queue\Transport\TransportInterface;

/**
 * Contract test su un Redis reale (REDIS_DSN, database 15 di default), con il livello forzato da level().
 * Ogni test usa un prefisso casuale e cancella le proprie chiavi.
 */
abstract class RedisTransportTestCase extends TransportContractTestCase
{
    private ?RedisClientInterface $redisClient = null;

    private ?QueueConfig $queueConfig = null;

    private SystemClock $systemClock;

    #[Override]
    protected function setUp(): void
    {
        $this->systemClock = new SystemClock();
        $redisClient = $this->connect();
        $server = (new VersionDetector($redisClient))->serverVersion();
        if (null === $server || !RedisVersionEnum::fromServerVersion($server)->isAtLeast($this->level())) {
            self::markTestSkipped(\sprintf('Redis %s does not support level %s.', $server ?? 'unknown', $this->level()->value));
        }

        $this->redisClient = $redisClient;
    }

    #[Override]
    protected function tearDown(): void
    {
        if (!$this->redisClient instanceof RedisClientInterface || !$this->queueConfig instanceof QueueConfig) {
            return;
        }

        $cursor = '0';
        do {
            $reply = $this->redisClient->execute(['SCAN', $cursor, 'MATCH', $this->queueConfig->prefix.':*', 'COUNT', '1000']);
            self::assertIsArray($reply);
            [$cursor, $keys] = $reply;
            self::assertIsString($cursor);
            self::assertIsArray($keys);
            if ([] !== $keys) {
                $this->redisClient->execute(['DEL', ...array_values(array_map(static fn (mixed $key): string => \is_string($key) ? $key : '', $keys))]);
            }
        } while ('0' !== $cursor);
    }

    abstract protected function level(): RedisVersionEnum;

    protected function connect(): RedisClientInterface
    {
        [$host, $port, $database] = self::dsn();

        return new PhpRedisClient(static function () use ($host, $port, $database): Redis {
            $redis = new Redis();

            try {
                $redis->connect($host, $port, 1.0);
                $redis->select($database);
            } catch (RedisException $redisException) {
                self::markTestSkipped('Redis is not reachable: '.$redisException->getMessage());
            }

            return $redis;
        });
    }

    /**
     * @return array{string, int, int}
     */
    protected static function dsn(): array
    {
        $dsn = getenv('REDIS_DSN');
        $parts = parse_url(\is_string($dsn) && '' !== $dsn ? $dsn : 'redis://127.0.0.1:6379/15');
        $database = isset($parts['path']) && '' !== \trim($parts['path'], '/') ? (int) \trim($parts['path'], '/') : 15;

        return [$parts['host'] ?? '127.0.0.1', $parts['port'] ?? 6379, $database];
    }

    #[Override]
    protected function createTransport(QueueConfig $queueConfig): TransportInterface
    {
        $this->queueConfig = new QueueConfig(
            name: $queueConfig->name,
            prefix: 'it'.bin2hex(random_bytes(6)),
            maxTextBytes: $queueConfig->maxTextBytes,
            maxHeadersBytes: $queueConfig->maxHeadersBytes,
            maxAttempts: $queueConfig->maxAttempts,
            maxDeliveries: $queueConfig->maxDeliveries,
            claimIdleMs: $queueConfig->claimIdleMs,
            dlqMaxLength: $queueConfig->dlqMaxLength,
        );

        $redisTransport = $this->transportFor($this->queueConfig);
        $redisTransport->setup();

        return $redisTransport;
    }

    #[Override]
    protected function createConsumer(TransportInterface $transport): TransportInterface
    {
        self::assertInstanceOf(QueueConfig::class, $this->queueConfig);

        return $this->transportFor($this->queueConfig);
    }

    #[Override]
    protected function advanceTimeMs(int $milliseconds): void
    {
        // L'inattività è misurata dal server: un piccolo margine evita confronti al millisecondo.
        usleep(($milliseconds + 25) * 1000);
    }

    #[Override]
    protected function clock(): ClockInterface
    {
        return $this->systemClock;
    }

    private function transportFor(QueueConfig $queueConfig): RedisTransport
    {
        self::assertInstanceOf(RedisClientInterface::class, $this->redisClient);

        return new RedisTransport(
            $this->redisClient,
            FeatureSelector::select(RedisCapabilities::forVersion($this->level())),
            'consumer-'.bin2hex(random_bytes(4)),
            $queueConfig,
            $this->systemClock,
        );
    }
}
