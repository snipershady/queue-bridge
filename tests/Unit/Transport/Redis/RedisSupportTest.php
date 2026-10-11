<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Message\MessageId;
use QueueBridge\Tests\Support\RecordingRedisClient;
use QueueBridge\Transport\Redis\RedisKeys;
use QueueBridge\Transport\Redis\RedisReply;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;
use RuntimeException;

#[CoversClass(RedisKeys::class)]
#[CoversClass(RedisReply::class)]
#[CoversClass(ScriptRunner::class)]
#[CoversClass(RedisScripts::class)]
#[CoversClass(CommandFailedException::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(MessageId::class)]
final class RedisSupportTest extends TestCase
{
    public function testKeysShareTheQueueHashTag(): void
    {
        $redisKeys = new RedisKeys(new QueueConfig(name: 'messages', prefix: 'rq'));

        self::assertSame('rq:{messages}:stream', $redisKeys->stream());
        self::assertSame('rq:{messages}:delayed', $redisKeys->delayed());
        self::assertSame('rq:{messages}:dlq', $redisKeys->deadLetters());
        self::assertSame(
            'rq:{messages}:idem:0190f2a4-5b6c-7d8e-9f01-23456789abcd',
            $redisKeys->idempotency(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd')),
        );
        self::assertSame('queue-bridge', $redisKeys->group());
    }

    public function testReplyNarrowing(): void
    {
        self::assertSame(['a', 1], RedisReply::list(['a', 1], 'ctx'));
        self::assertSame('x', RedisReply::string('x', 'ctx'));
        self::assertSame('42', RedisReply::string(42, 'ctx'));
        self::assertSame(42, RedisReply::int(42, 'ctx'));
        self::assertSame(-3, RedisReply::int('-3', 'ctx'));
        self::assertSame(['a' => '1', 'b' => ['x']], RedisReply::pairs(['a', '1', 'b', ['x']], 'ctx'));
        self::assertSame(['a' => '1', 'b' => '2'], RedisReply::fields(['a', '1', 'b', 2], 'ctx'));
        self::assertSame([], RedisReply::pairs([], 'ctx'));
    }

    /**
     * @return iterable<string, array{callable(): mixed, string}>
     */
    public static function invalidReplies(): iterable
    {
        yield 'list from string' => [static fn (): array => RedisReply::list('x', 'XLEN'), 'Unexpected reply for XLEN: expected an array.'];
        yield 'list from map' => [static fn (): array => RedisReply::list(['k' => 'v'], 'ctx'), 'expected an array'];
        yield 'string from array' => [static fn (): string => RedisReply::string([], 'ctx'), 'expected a string'];
        yield 'string from null' => [static fn (): string => RedisReply::string(reply: null, context: 'ctx'), 'expected a string'];
        yield 'int from text' => [static fn (): int => RedisReply::int('1.5', 'ctx'), 'expected an integer'];
        yield 'int from null' => [static fn (): int => RedisReply::int(reply: null, context: 'ctx'), 'expected an integer'];
        yield 'odd pairs' => [static fn (): array => RedisReply::pairs(['a'], 'ctx'), 'expected an even number of elements'];
        yield 'non string field' => [static fn (): array => RedisReply::fields(['a', ['x']], 'ctx'), 'expected a string'];
    }

    /**
     * @param callable(): mixed $call
     */
    #[DataProvider('invalidReplies')]
    public function testReplyNarrowingRejectsUnexpectedShapes(callable $call, string $error): void
    {
        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage($error);

        $call();
    }

    public function testCommandFailedExceptionMessages(): void
    {
        $previous = new RuntimeException('cause');
        $commandFailedException = CommandFailedException::rejected('XADD', 'WRONGTYPE', $previous);

        self::assertSame('XADD failed: WRONGTYPE', $commandFailedException->getMessage());
        self::assertSame($previous, $commandFailedException->getPrevious());
        self::assertSame(0, $commandFailedException->getCode());
    }

    public function testScriptRunnerUsesEvalshaFirst(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('EVALSHA', 7);

        self::assertSame(7, (new ScriptRunner($recordingRedisClient))->run('return 7', ['k1', 'k2'], ['a']));
        self::assertSame([['EVALSHA', sha1('return 7'), '2', 'k1', 'k2', 'a']], $recordingRedisClient->commands);
    }

    public function testScriptRunnerFallsBackToEvalOnNoscript(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())
            ->reply('EVALSHA', CommandFailedException::rejected('EVALSHA', 'NOSCRIPT No matching script.'))
            ->reply('EVAL', 'ok');

        self::assertSame('ok', (new ScriptRunner($recordingRedisClient))->run('return "ok"', [], []));
        self::assertSame(['EVALSHA', 'EVAL'], $recordingRedisClient->names());
        self::assertSame(['EVAL', 'return "ok"', '0'], $recordingRedisClient->commands[1]);
    }

    public function testScriptRunnerRethrowsOtherErrors(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('EVALSHA', CommandFailedException::rejected('EVALSHA', 'ERR boom'));

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('ERR boom');

        (new ScriptRunner($recordingRedisClient))->run('return 1', [], []);
    }

    public function testScriptsAreSelfContainedLua(): void
    {
        foreach ([RedisScripts::ack(), RedisScripts::settle(), RedisScripts::requeueOrphans()] as $script) {
            self::assertStringContainsString('local function owned(', $script);
            self::assertStringContainsString("redis.call('XACKDEL'", $script);
        }

        foreach ([RedisScripts::promoteDelayed(), RedisScripts::requeueDeadLetters(), RedisScripts::purgeDeadLetters(), RedisScripts::purgeReady(), RedisScripts::purgeDelayed(), RedisScripts::releaseLock()] as $script) {
            self::assertStringNotContainsString('local function owned(', $script);
            self::assertStringContainsString('redis.call(', $script);
        }

        self::assertStringContainsString('cjson.decode', RedisScripts::promoteDelayed());
        self::assertStringContainsString("'failure_'", RedisScripts::requeueDeadLetters());
        self::assertStringContainsString("redis.call('GET', KEYS[1]) == ARGV[1]", RedisScripts::releaseLock());
        self::assertStringContainsString("redis.call('DEL', KEYS[1])", RedisScripts::purgeDeadLetters());
        self::assertStringContainsString("redis.pcall('XPENDING'", RedisScripts::purgeReady());
        self::assertStringContainsString("redis.call('ZCARD', KEYS[1])", RedisScripts::purgeDelayed());
    }
}
