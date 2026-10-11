<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis\Client;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Tests\Support\CallLog;
use QueueBridge\Transport\Redis\Client\PhpRedisClient;
use Redis;
use RedisException;
use stdClass;

#[CoversClass(PhpRedisClient::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(ConnectionException::class)]
#[RequiresPhpExtension('redis')]
final class PhpRedisClientTest extends TestCase
{
    public function testNormalizesReplies(): void
    {
        $redis = $this->redis();
        $redis->method('getLastError')->willReturn(null);
        $redis->method('rawcommand')->willReturnOnConsecutiveCalls(true, false, 7, 'x', [1, false, ['a', true]], 1.5);

        $phpRedisClient = new PhpRedisClient($redis);

        self::assertSame('OK', $phpRedisClient->execute(['SET', 'k', 'v']));
        self::assertNull($phpRedisClient->execute(['GET', 'missing']));
        self::assertSame(7, $phpRedisClient->execute(['INCR', 'k']));
        self::assertSame('x', $phpRedisClient->execute(['GET', 'k']));
        self::assertSame([1, null, ['a', 'OK']], $phpRedisClient->execute(['EVAL', 'x', '0']));
        self::assertSame('1.5', $phpRedisClient->execute(['INCRBYFLOAT', 'k', '0.5']));
    }

    public function testRejectsUnknownReplyTypes(): void
    {
        $redis = $this->redis();
        $redis->method('rawcommand')->willReturn(new stdClass());

        $this->expectException(CommandFailedException::class);

        (new PhpRedisClient($redis))->execute(['GET', 'k']);
    }

    public function testServerErrorsBecomeCommandFailedException(): void
    {
        $redis = $this->redis();
        $redis->method('rawcommand')->willReturn(false);
        $redis->method('getLastError')->willReturn('WRONGTYPE Operation against a key holding the wrong kind of value');
        $redis->expects(self::exactly(2))->method('clearLastError');

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('HSET failed: WRONGTYPE');

        (new PhpRedisClient($redis))->execute(['HSET', 'k', 'f', 'v']);
    }

    public function testPipelineReportsErrorsPerCommand(): void
    {
        $redis = $this->redis();
        $redis->method('multi')->with(Redis::PIPELINE)->willReturnSelf();
        $redis->expects(self::exactly(2))->method('rawcommand');
        $redis->method('exec')->willReturn(['1-0', false]);
        $redis->method('getLastError')->willReturn('OOM');

        $results = (new PhpRedisClient($redis))->pipeline([['XADD', 's', '*', 'a', '1'], ['XADD', 's', '*', 'b', '2']]);

        self::assertSame('1-0', $results[0]);
        self::assertInstanceOf(CommandFailedException::class, $results[1]);
        self::assertSame('XADD failed: OOM', $results[1]->getMessage());
    }

    public function testPipelineWithoutRepliesMarksEveryCommandAsFailed(): void
    {
        $redis = $this->redis();
        $redis->method('multi')->willReturnSelf();
        $redis->method('exec')->willReturn(false);
        $redis->method('getLastError')->willReturn(null);

        $results = (new PhpRedisClient($redis))->pipeline([['ZADD', 'z', '1', 'm']]);

        self::assertInstanceOf(CommandFailedException::class, $results[0]);
        self::assertSame('ZADD failed: unknown error', $results[0]->getMessage());
    }

    public function testPipelineThatCannotStartMarksEveryCommandAsFailed(): void
    {
        $redis = $this->redis();
        $redis->method('multi')->willReturn(false);
        $redis->expects(self::never())->method('exec');
        $redis->method('getLastError')->willReturn('ERR pipeline unavailable');

        $results = (new PhpRedisClient($redis))->pipeline([['XADD', 's', '*', 'a', '1']]);

        self::assertInstanceOf(CommandFailedException::class, $results[0]);
        self::assertSame('XADD failed: ERR pipeline unavailable', $results[0]->getMessage());
    }

    public function testConnectionErrorWithoutFactory(): void
    {
        $redis = $this->redis();
        $redis->method('rawcommand')->willThrowException(new RedisException('Connection lost'));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Redis backend is unreachable: Connection lost');

        (new PhpRedisClient($redis))->execute(['PING']);
    }

    public function testFactoryIsUsedLazilyAndToReconnectOnce(): void
    {
        $broken = $this->redis();
        $broken->method('rawcommand')->willThrowException(new RedisException('Connection lost'));
        $healthy = $this->redis();
        $healthy->method('rawcommand')->willReturn('PONG');

        $callLog = new CallLog();
        $phpRedisClient = new PhpRedisClient(static function () use ($callLog, $broken, $healthy): Redis {
            $callLog->record('create');

            return 1 === \count($callLog->calls) ? $broken : $healthy;
        });
        self::assertSame([], $callLog->calls);

        self::assertSame('PONG', $phpRedisClient->execute(['PING']));
        self::assertSame('PONG', $phpRedisClient->execute(['PING']));
        self::assertSame(['create', 'create'], $callLog->calls);
    }

    public function testReconnectFailureBecomesConnectionException(): void
    {
        $broken = $this->redis();
        $broken->method('rawcommand')->willThrowException(new RedisException('refused'));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('refused');

        (new PhpRedisClient(static fn (): Redis => $broken))->execute(['PING']);
    }

    /**
     * @return MockObject&Redis
     */
    private function redis(): MockObject
    {
        return $this->createMock(Redis::class);
    }
}
