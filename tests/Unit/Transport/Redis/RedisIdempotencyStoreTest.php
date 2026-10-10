<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Transport\Redis;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\VersionMismatchException;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\Tests\Support\FakeRandom;
use snipershady\Queue\Tests\Support\RecordingRedisClient;
use snipershady\Queue\Transport\Redis\Feature\DelexLockRelease;
use snipershady\Queue\Transport\Redis\RedisIdempotencyStore;
use snipershady\Queue\Transport\Redis\RedisKeys;

#[CoversClass(RedisIdempotencyStore::class)]
#[CoversClass(VersionMismatchException::class)]
#[UsesClass(RedisKeys::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(LockToken::class)]
#[UsesClass(DelexLockRelease::class)]
#[UsesClass(NativeRandom::class)]
final class RedisIdempotencyStoreTest extends TestCase
{
    private const string KEY = 'rq:{q}:idem:0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    private MessageId $messageId;

    #[Override]
    protected function setUp(): void
    {
        $this->messageId = MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd');
    }

    public function testAcquireSetsTheLockOnlyIfAbsent(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('SET', 'OK')->reply('SET', reply: null);
        $redisIdempotencyStore = $this->store($recordingRedisClient);

        $lockToken = $redisIdempotencyStore->acquire($this->messageId, 1500);

        self::assertInstanceOf(LockToken::class, $lockToken);
        self::assertSame(str_repeat('ab', 16), $lockToken->token);
        self::assertSame(['SET', self::KEY, str_repeat('ab', 16), 'NX', 'PX', '1500'], $recordingRedisClient->commands[0]);
        self::assertNull($redisIdempotencyStore->acquire($this->messageId, 1500));
    }

    public function testCompleteMarksTheMessageAsDone(): void
    {
        $recordingRedisClient = new RecordingRedisClient();

        $this->store($recordingRedisClient)->complete(new LockToken($this->messageId, 'token'), 60_000);

        self::assertSame([['SET', self::KEY, 'done', 'PX', '60000']], $recordingRedisClient->commands);
    }

    public function testReleaseUsesTheLockReleaseStrategy(): void
    {
        $recordingRedisClient = new RecordingRedisClient();

        $this->store($recordingRedisClient)->release(new LockToken($this->messageId, 'token'));

        self::assertSame([['DELEX', self::KEY, 'IFEQ', 'token']], $recordingRedisClient->commands);
    }

    public function testDefaultsUseTheDefaultQueueAndANativeRandomToken(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('SET', 'OK');

        $lockToken = (new RedisIdempotencyStore($recordingRedisClient, new DelexLockRelease()))->acquire($this->messageId, 1);

        self::assertInstanceOf(LockToken::class, $lockToken);
        self::assertSame(32, \strlen($lockToken->token));
        self::assertSame('rq:{default}:idem:'.$this->messageId->value, $recordingRedisClient->commands[0][1]);
    }

    public function testTtlsMustBePositive(): void
    {
        $redisIdempotencyStore = $this->store(new RecordingRedisClient());

        foreach ([
            fn (): ?LockToken => $redisIdempotencyStore->acquire($this->messageId, 0),
            fn (): null => $redisIdempotencyStore->complete(new LockToken($this->messageId, 't'), 0),
        ] as $call) {
            try {
                $call();
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertSame('TTL must be >= 1 ms, got 0.', $invalidArgumentException->getMessage());
            }
        }
    }

    public function testVersionMismatchMessages(): void
    {
        self::assertStringContainsString('Requested Redis level 8.4', VersionMismatchException::overrideAboveServer('8.4', '7.2.4')->getMessage());
        self::assertSame('Redis 5.0 is not supported: version 6.0 or later is required.', VersionMismatchException::unsupported('5.0', '6.0')->getMessage());
    }

    private function store(RecordingRedisClient $recordingRedisClient): RedisIdempotencyStore
    {
        return new RedisIdempotencyStore($recordingRedisClient, new DelexLockRelease(), new QueueConfig(name: 'q'), new FakeRandom(0xAB));
    }
}
