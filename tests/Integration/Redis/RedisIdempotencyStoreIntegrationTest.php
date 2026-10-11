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
use snipershady\Queue\Idempotency\AcquireResult;
use snipershady\Queue\Idempotency\AcquireStatus;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\System\UuidV7Generator;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisIdempotencyStore;
use snipershady\Queue\Transport\Redis\Script\RedisScripts;
use snipershady\Queue\Transport\Redis\Version\VersionDetector;

/**
 * Lo script di acquisizione distingue "in elaborazione" da "completato" su un Redis reale.
 */
#[Group('redis')]
#[CoversClass(RedisIdempotencyStore::class)]
#[CoversClass(RedisScripts::class)]
#[UsesClass(AcquireResult::class)]
#[UsesClass(PhpRedisClient::class)]
#[UsesClass(VersionDetector::class)]
final class RedisIdempotencyStoreIntegrationTest extends TestCase
{
    private RedisClientInterface $redisClient;

    private RedisIdempotencyStore $redisIdempotencyStore;

    private MessageId $messageId;

    private string $key;

    #[Override]
    protected function setUp(): void
    {
        $dsn = getenv('REDIS_DSN');
        $parts = parse_url(\is_string($dsn) && '' !== $dsn ? $dsn : 'redis://127.0.0.1:6379/15');
        $database = isset($parts['path']) && '' !== \trim($parts['path'], '/') ? (int) \trim($parts['path'], '/') : 15;
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

        $prefix = 'it'.bin2hex(random_bytes(6));
        $this->redisIdempotencyStore = RedisIdempotencyStore::create($this->redisClient, new QueueConfig(name: 'idem', prefix: $prefix));
        $this->messageId = (new UuidV7Generator(new SystemClock()))->generate();
        $this->key = \sprintf('%s:{idem}:idem:%s', $prefix, $this->messageId->value);
    }

    #[Override]
    protected function tearDown(): void
    {
        $this->redisClient->execute(['DEL', $this->key]);
    }

    public function testLockIsExclusiveAndReportsTheRemainingTime(): void
    {
        $lockToken = $this->acquire();

        $acquireResult = $this->redisIdempotencyStore->acquire($this->messageId, 60_000);
        self::assertSame(AcquireStatus::InProgress, $acquireResult->status);
        self::assertNull($acquireResult->token);
        self::assertGreaterThan(50_000, $acquireResult->lockRemainingMs);
        self::assertLessThanOrEqual(60_000, $acquireResult->lockRemainingMs);

        $this->redisIdempotencyStore->release($lockToken);
        $this->acquire();
    }

    public function testCompletedMessageIsNeverReportedAsInProgress(): void
    {
        $this->redisIdempotencyStore->complete($this->acquire(), 60_000);

        $acquireResult = $this->redisIdempotencyStore->acquire($this->messageId, 60_000);
        self::assertSame(AcquireStatus::Completed, $acquireResult->status);
        self::assertNull($acquireResult->token);
    }

    public function testReleaseWithAnotherTokenKeepsTheLock(): void
    {
        $this->acquire();
        $this->redisIdempotencyStore->release(new LockToken($this->messageId, 'someone-else'));

        self::assertSame(AcquireStatus::InProgress, $this->redisIdempotencyStore->acquire($this->messageId, 60_000)->status);
    }

    private function acquire(): LockToken
    {
        $acquireResult = $this->redisIdempotencyStore->acquire($this->messageId, 60_000);
        self::assertSame(AcquireStatus::Acquired, $acquireResult->status);
        self::assertInstanceOf(LockToken::class, $acquireResult->token);

        return $acquireResult->token;
    }
}
