<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Redis;
use RedisException;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\VersionMismatchException;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\Message;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\System\UuidV7Generator;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Feature\FeatureSelector;
use snipershady\Queue\Transport\Redis\RedisTransport;
use snipershady\Queue\Transport\Redis\Version\RedisCapabilities;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;
use snipershady\Queue\Transport\Redis\Version\VersionDetector;

/**
 * Auto-rilevamento e override sul server reale, nessuna perdita di messaggi con il trim.
 */
#[Group('redis')]
#[CoversClass(VersionDetector::class)]
#[CoversClass(RedisTransport::class)]
#[UsesClass(PhpRedisClient::class)]
#[UsesClass(FeatureSelector::class)]
final class RedisVersionAndTrimTest extends TestCase
{
    private RedisClientInterface $redisClient;

    private string $server;

    private string $prefix;

    #[Override]
    protected function setUp(): void
    {
        $dsn = getenv('REDIS_DSN');
        $parts = parse_url(\is_string($dsn) && '' !== $dsn ? $dsn : 'redis://127.0.0.1:6379/15');
        $database = isset($parts['path']) && '' !== trim($parts['path'], '/') ? (int) trim($parts['path'], '/') : 15;
        $host = $parts['host'] ?? '127.0.0.1';
        $port = $parts['port'] ?? 6379;

        $this->redisClient = new PhpRedisClient(static function () use ($host, $port, $database): Redis {
            $redis = new Redis();

            try {
                $redis->connect($host, $port, 1.0);
                $redis->select($database);
            } catch (RedisException $redisException) {
                self::markTestSkipped('Redis is not reachable: '.$redisException->getMessage());
            }

            return $redis;
        });

        $server = (new VersionDetector($this->redisClient))->serverVersion();
        self::assertIsString($server);
        $this->server = $server;
        $this->prefix = 'it'.bin2hex(random_bytes(6));
    }

    #[Override]
    protected function tearDown(): void
    {
        foreach (['stream', 'delayed', 'dlq'] as $suffix) {
            $this->redisClient->execute(['DEL', \sprintf('%s:{trim}:%s', $this->prefix, $suffix)]);
        }
    }

    public function testAutoDetectionMatchesTheServer(): void
    {
        self::assertSame(RedisVersionEnum::fromServerVersion($this->server), (new VersionDetector($this->redisClient))->resolve());
    }

    public function testOverrideAboveTheServerIsRejected(): void
    {
        $detected = RedisVersionEnum::fromServerVersion($this->server);
        if (RedisVersionEnum::GE84 === $detected) {
            self::markTestSkipped('No level above the server to test.');
        }

        $this->expectException(VersionMismatchException::class);

        (new VersionDetector($this->redisClient))->resolve(RedisVersionEnum::GE84);
    }

    public function testOverrideNotAboveTheServerIsAccepted(): void
    {
        self::assertSame(RedisVersionEnum::GE60, (new VersionDetector($this->redisClient))->resolve(RedisVersionEnum::GE60));
    }

    public function testTrimNeverLosesUnreadOrPendingMessages(): void
    {
        $systemClock = new SystemClock();
        $level = RedisVersionEnum::fromServerVersion($this->server);
        $queueConfig = new QueueConfig(name: 'trim', prefix: $this->prefix);
        $redisTransport = new RedisTransport($this->redisClient, FeatureSelector::select(RedisCapabilities::forVersion($level)), 'trim-consumer', $queueConfig, $systemClock);
        $redisTransport->setup();

        $uuidV7Generator = new UuidV7Generator($systemClock);

        for ($i = 0; $i < 300; ++$i) {
            $redisTransport->send(new Message($uuidV7Generator->generate(), 'test', 'm'.$i, $systemClock->now()));
        }

        // 80 confermati normalmente, 20 confermati con il solo XACK (residui da trimmare),
        // 50 in elaborazione, 150 mai letti: poi manutenzione, con trim, ripetuta.
        $redisTransport->ackBatch($redisTransport->receive(80, 0));
        $leftovers = $redisTransport->receive(20, 0);
        $this->redisClient->execute(['XACK', $this->prefix.':{trim}:stream', 'queue-bridge', ...array_map(static fn (Delivery $delivery): string => $delivery->receipt, $leftovers)]);
        $pending = $redisTransport->receive(50, 0);
        $trimmed = $redisTransport->maintain()->trimmed + $redisTransport->maintain()->trimmed;

        self::assertSame($level->isAtLeast(RedisVersionEnum::GE62) ? 20 : 0, $trimmed);

        $texts = [];
        foreach ($pending as $delivery) {
            $texts[] = $delivery->message->text;
            $redisTransport->ack($delivery);
        }

        while ([] !== ($batch = $redisTransport->receive(100, 0))) {
            foreach ($batch as $delivery) {
                $texts[] = $delivery->message->text;
            }

            $redisTransport->ackBatch($batch);
        }

        $expected = array_map(static fn (int $i): string => 'm'.$i, range(100, 299));
        sort($expected);
        sort($texts);
        self::assertSame($expected, $texts);
    }
}
