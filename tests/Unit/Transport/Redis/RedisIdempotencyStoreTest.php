<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\VersionMismatchException;
use QueueBridge\Idempotency\AcquireResult;
use QueueBridge\Idempotency\AcquireStatus;
use QueueBridge\Idempotency\LockToken;
use QueueBridge\Message\MessageId;
use QueueBridge\System\NativeRandom;
use QueueBridge\Tests\Support\FakeRandom;
use QueueBridge\Tests\Support\RecordingRedisClient;
use QueueBridge\Transport\Redis\Feature\DelexLockRelease;
use QueueBridge\Transport\Redis\RedisIdempotencyStore;
use QueueBridge\Transport\Redis\RedisKeys;
use QueueBridge\Transport\Redis\RedisReply;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;

#[CoversClass(RedisIdempotencyStore::class)]
#[CoversClass(VersionMismatchException::class)]
#[UsesClass(RedisKeys::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(LockToken::class)]
#[UsesClass(DelexLockRelease::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(AcquireResult::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(RedisReply::class)]
#[CoversClass(RedisScripts::class)]
#[UsesClass(ScriptRunner::class)]
final class RedisIdempotencyStoreTest extends TestCase
{
    private const string KEY = 'rq:{q}:idem:0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    private MessageId $messageId;

    #[Override]
    protected function setUp(): void
    {
        $this->messageId = MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd');
    }

    public function testAcquireRunsTheLockScriptAndReturnsTheToken(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('EVALSHA', [1, 0]);

        $acquireResult = $this->store($recordingRedisClient)->acquire($this->messageId, 1500);

        self::assertSame(AcquireStatus::Acquired, $acquireResult->status);
        self::assertInstanceOf(LockToken::class, $acquireResult->token);
        self::assertSame(str_repeat('ab', 16), $acquireResult->token->token);
        self::assertSame($this->messageId, $acquireResult->token->messageId);
        self::assertSame(
            [['EVALSHA', sha1(RedisScripts::acquireLock()), '1', self::KEY, str_repeat('ab', 16), '1500', 'done']],
            $recordingRedisClient->commands,
        );
    }

    public function testAcquireReportsACompletedMessage(): void
    {
        $acquireResult = $this->store((new RecordingRedisClient())->reply('EVALSHA', [2, 0]))->acquire($this->messageId, 1500);

        self::assertSame(AcquireStatus::Completed, $acquireResult->status);
        self::assertNull($acquireResult->token);
    }

    public function testAcquireReportsAMessageInProgressWithTheRemainingLockTime(): void
    {
        $acquireResult = $this->store((new RecordingRedisClient())->reply('EVALSHA', [0, 1234]))->acquire($this->messageId, 1500);

        self::assertSame(AcquireStatus::InProgress, $acquireResult->status);
        self::assertNull($acquireResult->token);
        self::assertSame(1234, $acquireResult->lockRemainingMs);
    }

    public function testAcquireRetriesSoonWhenTheOtherLockHasNoValidTtl(): void
    {
        foreach ([0, -1, -2] as $pttl) {
            $acquireResult = $this->store((new RecordingRedisClient())->reply('EVALSHA', [0, $pttl]))->acquire($this->messageId, 1500);

            self::assertSame(AcquireStatus::InProgress, $acquireResult->status);
            self::assertSame(1, $acquireResult->lockRemainingMs);
        }
    }

    public function testAcquireFallsBackToEvalWhenTheScriptIsNotCached(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())
            ->reply('EVALSHA', CommandFailedException::rejected('EVALSHA', 'NOSCRIPT No matching script.'))
            ->reply('EVAL', [1, 0]);

        $acquireResult = $this->store($recordingRedisClient)->acquire($this->messageId, 1500);

        self::assertSame(AcquireStatus::Acquired, $acquireResult->status);
        self::assertSame(['EVALSHA', 'EVAL'], $recordingRedisClient->names());
        self::assertSame(RedisScripts::acquireLock(), $recordingRedisClient->commands[1][1]);
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
        $recordingRedisClient = (new RecordingRedisClient())->reply('EVALSHA', [1, 0]);

        $lockToken = (new RedisIdempotencyStore($recordingRedisClient, new DelexLockRelease()))->acquire($this->messageId, 1)->token;

        self::assertInstanceOf(LockToken::class, $lockToken);
        self::assertSame(32, \strlen($lockToken->token));
        self::assertSame('rq:{default}:idem:'.$this->messageId->value, $recordingRedisClient->commands[0][3]);
    }

    public function testTtlsMustBePositive(): void
    {
        $redisIdempotencyStore = $this->store(new RecordingRedisClient());

        foreach ([
            fn (): AcquireResult => $redisIdempotencyStore->acquire($this->messageId, 0),
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
