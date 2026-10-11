<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis\Feature;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Tests\Support\RecordingRedisClient;
use QueueBridge\Transport\Redis\Feature\AckDelStrategy;
use QueueBridge\Transport\Redis\Feature\AckedTrimStrategy;
use QueueBridge\Transport\Redis\Feature\AckThenDeleteStrategy;
use QueueBridge\Transport\Redis\Feature\DelexLockRelease;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\Feature\GroupInfo;
use QueueBridge\Transport\Redis\Feature\GroupInfoLagStrategy;
use QueueBridge\Transport\Redis\Feature\LuaLockRelease;
use QueueBridge\Transport\Redis\Feature\MinIdTrimStrategy;
use QueueBridge\Transport\Redis\Feature\NoTrimStrategy;
use QueueBridge\Transport\Redis\Feature\NullLagStrategy;
use QueueBridge\Transport\Redis\Feature\PendingClaimStrategy;
use QueueBridge\Transport\Redis\Feature\PendingEntries;
use QueueBridge\Transport\Redis\Feature\PendingIdleStrategy;
use QueueBridge\Transport\Redis\Feature\ReadGroupClaimStrategy;
use QueueBridge\Transport\Redis\Feature\RedisFeatures;
use QueueBridge\Transport\Redis\RedisKeys;
use QueueBridge\Transport\Redis\RedisReply;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;
use QueueBridge\Transport\Redis\Version\RedisCapabilities;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;

#[CoversClass(PendingClaimStrategy::class)]
#[CoversClass(PendingIdleStrategy::class)]
#[CoversClass(ReadGroupClaimStrategy::class)]
#[CoversClass(PendingEntries::class)]
#[CoversClass(AckThenDeleteStrategy::class)]
#[CoversClass(AckDelStrategy::class)]
#[CoversClass(NoTrimStrategy::class)]
#[CoversClass(MinIdTrimStrategy::class)]
#[CoversClass(AckedTrimStrategy::class)]
#[CoversClass(GroupInfo::class)]
#[CoversClass(NullLagStrategy::class)]
#[CoversClass(GroupInfoLagStrategy::class)]
#[CoversClass(LuaLockRelease::class)]
#[CoversClass(DelexLockRelease::class)]
#[CoversClass(RedisFeatures::class)]
#[CoversClass(FeatureSelector::class)]
#[UsesClass(RedisKeys::class)]
#[UsesClass(RedisReply::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(ScriptRunner::class)]
#[UsesClass(RedisScripts::class)]
#[UsesClass(RedisCapabilities::class)]
#[UsesClass(RedisVersionEnum::class)]
final class RedisFeaturesTest extends TestCase
{
    private const array PENDING = [
        ['1-0', 'consumer-a', 5000, 1],
        ['2-0', 'consumer-b', 999, 3],
        ['3-0', 'consumer-a', 1000, 2],
    ];

    private RedisKeys $redisKeys;

    #[Override]
    protected function setUp(): void
    {
        $this->redisKeys = new RedisKeys(new QueueConfig(name: 'q', prefix: 'rq'));
    }

    public function testPendingClaimFiltersIdleEntriesClientSide(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XPENDING', self::PENDING);
        $pendingClaimStrategy = new PendingClaimStrategy();

        self::assertSame(['1-0', '3-0'], $pendingClaimStrategy->findIdle($recordingRedisClient, $this->redisKeys, 1000, 50));
        self::assertSame([['XPENDING', 'rq:{q}:stream', 'queue-bridge', '-', '+', '50']], $recordingRedisClient->commands);
        self::assertFalse($pendingClaimStrategy->claimsOnRead());
        self::assertSame([], $pendingClaimStrategy->readArguments(1000));
    }

    public function testPendingIdleAsksTheServerToFilter(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XPENDING', [self::PENDING[0]]);
        $pendingIdleStrategy = new PendingIdleStrategy();

        self::assertSame(['1-0'], $pendingIdleStrategy->findIdle($recordingRedisClient, $this->redisKeys, 1000, 50));
        self::assertSame([['XPENDING', 'rq:{q}:stream', 'queue-bridge', 'IDLE', '1000', '-', '+', '50']], $recordingRedisClient->commands);
        self::assertFalse($pendingIdleStrategy->claimsOnRead());
        self::assertSame([], $pendingIdleStrategy->readArguments(1000));
    }

    public function testReadGroupClaimRecoversOrphansWhileReading(): void
    {
        $recordingRedisClient = new RecordingRedisClient();
        $readGroupClaimStrategy = new ReadGroupClaimStrategy();

        self::assertTrue($readGroupClaimStrategy->claimsOnRead());
        self::assertSame(['CLAIM', '30000'], $readGroupClaimStrategy->readArguments(30_000));
        self::assertSame([], $readGroupClaimStrategy->findIdle($recordingRedisClient, $this->redisKeys, 1000, 50));
        self::assertSame([], $recordingRedisClient->commands);
    }

    public function testPendingEntriesRejectsMalformedReplies(): void
    {
        $this->expectException(CommandFailedException::class);

        PendingEntries::idleIds([['1-0', 'consumer']], 0);
    }

    public function testAckModes(): void
    {
        self::assertSame('xack_xdel', (new AckThenDeleteStrategy())->mode());
        self::assertSame('xackdel', (new AckDelStrategy())->mode());
    }

    public function testNoTrimNeverTouchesTheStream(): void
    {
        $recordingRedisClient = new RecordingRedisClient();

        self::assertSame(0, (new NoTrimStrategy())->trim($recordingRedisClient, $this->redisKeys));
        self::assertSame([], $recordingRedisClient->commands);
    }

    public function testAckedTrimRemovesOnlyAcknowledgedEntries(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XTRIM', 4);

        self::assertSame(4, (new AckedTrimStrategy())->trim($recordingRedisClient, $this->redisKeys));
        self::assertSame([['XTRIM', 'rq:{q}:stream', 'MAXLEN', '0', 'ACKED']], $recordingRedisClient->commands);
    }

    public function testMinIdTrimUsesTheOldestPendingEntry(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())
            ->reply('XINFO', [$this->group(['last-delivered-id', '10-3'])])
            ->reply('XPENDING', [2, '7-0', '10-3', [['consumer-a', '2']]])
            ->reply('XTRIM', 5);

        self::assertSame(5, (new MinIdTrimStrategy())->trim($recordingRedisClient, $this->redisKeys));
        self::assertSame(['XTRIM', 'rq:{q}:stream', 'MINID', '7-0'], $recordingRedisClient->commands[2]);
        self::assertSame(['XPENDING', 'rq:{q}:stream', 'queue-bridge'], $recordingRedisClient->commands[1]);
    }

    public function testMinIdTrimWithoutPendingKeepsEntriesAfterTheLastDelivered(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())
            ->reply('XINFO', [$this->group(['last-delivered-id', '10-3'])])
            ->reply('XPENDING', [0, null, null, null])
            ->reply('XTRIM', 1);

        self::assertSame(1, (new MinIdTrimStrategy())->trim($recordingRedisClient, $this->redisKeys));
        self::assertSame(['XTRIM', 'rq:{q}:stream', 'MINID', '10-4'], $recordingRedisClient->commands[2]);
    }

    public function testMinIdTrimDoesNothingBeforeAnyDelivery(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XINFO', [$this->group(['last-delivered-id', '0-0'])]);

        self::assertSame(0, (new MinIdTrimStrategy())->trim($recordingRedisClient, $this->redisKeys));
        self::assertSame(['XINFO'], $recordingRedisClient->names());
    }

    public function testMinIdTrimDoesNothingWithoutTheGroup(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XINFO', [['name', 'other-group']]);

        self::assertSame(0, (new MinIdTrimStrategy())->trim($recordingRedisClient, $this->redisKeys));
    }

    public function testMinIdTrimRejectsMalformedIds(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())
            ->reply('XINFO', [$this->group(['last-delivered-id', 'garbage'])])
            ->reply('XPENDING', [0, null, null, null]);

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('expected ms-seq');

        (new MinIdTrimStrategy())->trim($recordingRedisClient, $this->redisKeys);
    }

    public function testGroupInfoTreatsAMissingStreamAsNoGroup(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XINFO', CommandFailedException::rejected('XINFO', 'ERR no such key'));

        self::assertNull(GroupInfo::find($recordingRedisClient, $this->redisKeys));
        self::assertSame([['XINFO', 'GROUPS', 'rq:{q}:stream']], $recordingRedisClient->commands);
    }

    public function testGroupInfoRethrowsOtherErrors(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('XINFO', CommandFailedException::rejected('XINFO', 'NOPERM'));

        $this->expectException(CommandFailedException::class);

        GroupInfo::find($recordingRedisClient, $this->redisKeys);
    }

    public function testLagStrategies(): void
    {
        self::assertNull((new NullLagStrategy())->lag(['lag' => 5]));
        self::assertSame(5, (new GroupInfoLagStrategy())->lag(['lag' => 5]));
        self::assertNull((new GroupInfoLagStrategy())->lag(['lag' => null]));
        self::assertNull((new GroupInfoLagStrategy())->lag([]));
    }

    public function testLuaLockReleaseComparesAndDeletesInAScript(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('EVALSHA', 1);

        (new LuaLockRelease())->release($recordingRedisClient, 'rq:{q}:idem:x', 'token');

        self::assertSame([['EVALSHA', sha1(RedisScripts::releaseLock()), '1', 'rq:{q}:idem:x', 'token']], $recordingRedisClient->commands);
    }

    public function testDelexLockReleaseUsesTheNativeCommand(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('DELEX', 1);

        (new DelexLockRelease())->release($recordingRedisClient, 'rq:{q}:idem:x', 'token');

        self::assertSame([['DELEX', 'rq:{q}:idem:x', 'IFEQ', 'token']], $recordingRedisClient->commands);
    }

    /**
     * @return iterable<string, array{RedisVersionEnum, list<class-string>}>
     */
    public static function selections(): iterable
    {
        yield '6.0' => [RedisVersionEnum::GE60, [PendingClaimStrategy::class, AckThenDeleteStrategy::class, NoTrimStrategy::class, NullLagStrategy::class, LuaLockRelease::class]];
        yield '6.2' => [RedisVersionEnum::GE62, [PendingIdleStrategy::class, AckThenDeleteStrategy::class, MinIdTrimStrategy::class, NullLagStrategy::class, LuaLockRelease::class]];
        yield '7.0' => [RedisVersionEnum::GE70, [PendingIdleStrategy::class, AckThenDeleteStrategy::class, MinIdTrimStrategy::class, GroupInfoLagStrategy::class, LuaLockRelease::class]];
        yield '8.0' => [RedisVersionEnum::GE80, [PendingIdleStrategy::class, AckThenDeleteStrategy::class, MinIdTrimStrategy::class, GroupInfoLagStrategy::class, LuaLockRelease::class]];
        yield '8.2' => [RedisVersionEnum::GE82, [PendingIdleStrategy::class, AckDelStrategy::class, AckedTrimStrategy::class, GroupInfoLagStrategy::class, LuaLockRelease::class]];
        yield '8.4' => [RedisVersionEnum::GE84, [ReadGroupClaimStrategy::class, AckDelStrategy::class, AckedTrimStrategy::class, GroupInfoLagStrategy::class, DelexLockRelease::class]];
    }

    /**
     * @param list<class-string> $expected
     */
    #[DataProvider('selections')]
    public function testFeatureSelectorPicksOneStrategyPerCapability(RedisVersionEnum $redisVersionEnum, array $expected): void
    {
        $redisCapabilities = RedisCapabilities::forVersion($redisVersionEnum);
        $redisFeatures = FeatureSelector::select($redisCapabilities);

        self::assertSame($redisCapabilities, $redisFeatures->capabilities);
        self::assertSame(
            [
                $redisFeatures->idleClaim::class,
                $redisFeatures->ack::class,
                $redisFeatures->trim::class,
                $redisFeatures->lag::class,
                $redisFeatures->lockRelease::class,
            ],
            $expected,
        );
    }

    /**
     * @param list<int|string|null> $extra
     *
     * @return list<int|string|null>
     */
    private function group(array $extra): array
    {
        return ['name', 'queue-bridge', 'consumers', 1, 'pending', 0, ...$extra];
    }
}
