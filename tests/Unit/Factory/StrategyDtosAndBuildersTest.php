<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Factory;

use InvalidArgumentException;
use Override;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;
use Redis;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Exception\VersionMismatchException;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Tests\Support\FakeAmqpConnector;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\FakeRandom;
use snipershady\Queue\Tests\Support\RecordingLogger;
use snipershady\Queue\Tests\Support\RecordingRedisClient;
use snipershady\Queue\Transport\InMemory\InMemoryBroker;
use snipershady\Queue\Transport\InMemory\InMemoryEntry;
use snipershady\Queue\Transport\InMemory\InMemoryTransport;
use snipershady\Queue\Transport\InMemory\InMemoryTransportBuilder;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use snipershady\Queue\Transport\RabbitMq\RabbitMqDelayModeEnum;
use snipershady\Queue\Transport\RabbitMq\RabbitMqTransport;
use snipershady\Queue\Transport\RabbitMq\RabbitMqTransportBuilder;
use snipershady\Queue\Transport\RabbitMq\RetryTiers;
use snipershady\Queue\Transport\RabbitMq\StrategyDtoRabbitMq;
use snipershady\Queue\Transport\RabbitMq\Topology;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Client\PredisClient;
use snipershady\Queue\Transport\Redis\Feature\DelexLockRelease;
use snipershady\Queue\Transport\Redis\Feature\FeatureSelector;
use snipershady\Queue\Transport\Redis\Feature\LuaLockRelease;
use snipershady\Queue\Transport\Redis\Feature\PendingClaimStrategy;
use snipershady\Queue\Transport\Redis\Feature\ReadGroupClaimStrategy;
use snipershady\Queue\Transport\Redis\Feature\RedisFeatures;
use snipershady\Queue\Transport\Redis\RedisIdempotencyStore;
use snipershady\Queue\Transport\Redis\RedisKeys;
use snipershady\Queue\Transport\Redis\RedisReply;
use snipershady\Queue\Transport\Redis\RedisTransport;
use snipershady\Queue\Transport\Redis\RedisTransportBuilder;
use snipershady\Queue\Transport\Redis\Script\ScriptRunner;
use snipershady\Queue\Transport\Redis\StrategyDtoRedis;
use snipershady\Queue\Transport\Redis\Version\RedisCapabilities;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;
use snipershady\Queue\Transport\Redis\Version\VersionDetector;

#[CoversClass(StrategyDtoInMemory::class)]
#[CoversClass(InMemoryTransportBuilder::class)]
#[CoversClass(StrategyDtoRedis::class)]
#[CoversClass(RedisTransportBuilder::class)]
#[CoversClass(StrategyDtoRabbitMq::class)]
#[CoversClass(RabbitMqTransportBuilder::class)]
#[CoversClass(RedisIdempotencyStore::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(RedisTransport::class)]
#[UsesClass(RedisKeys::class)]
#[UsesClass(FeatureSelector::class)]
#[UsesClass(VersionDetector::class)]
#[UsesClass(PhpRedisClient::class)]
#[UsesClass(PredisClient::class)]
#[UsesClass(RabbitMqTransport::class)]
#[UsesClass(Topology::class)]
#[UsesClass(RetryTiers::class)]
#[UsesClass(PhpAmqpLibConnector::class)]
#[UsesClass(UnsupportedStrategyException::class)]
#[UsesClass(VersionMismatchException::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(LockToken::class)]
#[UsesClass(DelexLockRelease::class)]
#[UsesClass(LuaLockRelease::class)]
#[UsesClass(ReadGroupClaimStrategy::class)]
#[UsesClass(RedisFeatures::class)]
#[UsesClass(RedisReply::class)]
#[UsesClass(ScriptRunner::class)]
#[UsesClass(RedisCapabilities::class)]
#[UsesClass(RedisVersionEnum::class)]
#[UsesClass(PendingClaimStrategy::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(InMemoryEntry::class)]
final class StrategyDtosAndBuildersTest extends TestCase
{
    private FakeClock $fakeClock;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock();
    }

    // --- InMemory -------------------------------------------------------------------------------------------

    public function testInMemoryBuilderUsesTheDtoClockOrItsOwn(): void
    {
        $inMemoryTransportBuilder = new InMemoryTransportBuilder($this->fakeClock);
        $dtoClock = new FakeClock('2030-01-01T00:00:00Z');

        self::assertSame('in-memory', (new StrategyDtoInMemory())->name());
        self::assertTrue($inMemoryTransportBuilder->supports(new StrategyDtoInMemory()));
        self::assertFalse($inMemoryTransportBuilder->supports($this->foreignDto()));
        self::assertInstanceOf(InMemoryTransport::class, $inMemoryTransportBuilder->build(new StrategyDtoInMemory($dtoClock), new QueueConfig()));
        self::assertInstanceOf(InMemoryTransport::class, $inMemoryTransportBuilder->build(new StrategyDtoInMemory(), new QueueConfig()));
        self::assertInstanceOf(InMemoryTransport::class, (new InMemoryTransportBuilder())->build(new StrategyDtoInMemory(), new QueueConfig()));
        self::assertSame($dtoClock, (new StrategyDtoInMemory($dtoClock))->clock);
    }

    public function testInMemoryTransportFollowsTheDtoClock(): void
    {
        $dtoClock = new FakeClock();
        $transport = (new InMemoryTransportBuilder($this->fakeClock))->build(new StrategyDtoInMemory($dtoClock), new QueueConfig());
        $transport->send(new Message(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd'), 'test', 'x', $dtoClock->now()), 1000);

        $this->fakeClock->advanceMs(5000);
        self::assertSame([], $transport->receive());

        $dtoClock->advanceMs(1000);
        self::assertCount(1, $transport->receive());
    }

    // --- Redis ----------------------------------------------------------------------------------------------

    public function testRedisDtoDefaultsAndValidation(): void
    {
        $recordingRedisClient = new RecordingRedisClient();
        $strategyDtoRedis = new StrategyDtoRedis($recordingRedisClient);

        self::assertSame('redis', $strategyDtoRedis->name());
        self::assertSame([$recordingRedisClient, null, true, null], [$strategyDtoRedis->client, $strategyDtoRedis->version, $strategyDtoRedis->verifyVersion, $strategyDtoRedis->consumer]);

        $this->expectException(InvalidArgumentException::class);
        new StrategyDtoRedis($recordingRedisClient, consumer: ' ');
    }

    #[RequiresPhpExtension('redis')]
    public function testFromPhpRedisWrapsTheNativeClient(): void
    {
        $strategyDtoRedis = StrategyDtoRedis::fromPhpRedis(self::createStub(Redis::class), RedisVersionEnum::GE70);
        $withFactory = StrategyDtoRedis::fromPhpRedis(static fn (): Redis => new Redis());

        self::assertInstanceOf(PhpRedisClient::class, $strategyDtoRedis->client);
        self::assertSame(RedisVersionEnum::GE70, $strategyDtoRedis->version);
        self::assertInstanceOf(PhpRedisClient::class, $withFactory->client);
        self::assertNull($withFactory->version);
    }

    public function testFromPredisWrapsTheNativeClient(): void
    {
        $strategyDtoRedis = StrategyDtoRedis::fromPredis(self::createStub(ClientInterface::class), RedisVersionEnum::GE62);

        self::assertInstanceOf(PredisClient::class, $strategyDtoRedis->client);
        self::assertSame(RedisVersionEnum::GE62, $strategyDtoRedis->version);
        self::assertNull(StrategyDtoRedis::fromPredis(self::createStub(ClientInterface::class))->version);
    }

    public function testRedisBuilderDetectsTheVersionAndNamesTheConsumer(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())
            ->reply('INFO', "redis_version:8.4.0\r\n")
            ->reply('EVALSHA', 0)
            ->reply('XREADGROUP', reply: null);
        $recordingLogger = new RecordingLogger();
        $redisTransportBuilder = new RedisTransportBuilder($this->fakeClock, $recordingLogger, new FakeRandom(0xAB));

        self::assertTrue($redisTransportBuilder->supports(new StrategyDtoRedis($recordingRedisClient)));
        self::assertFalse($redisTransportBuilder->supports($this->foreignDto()));

        $transport = $redisTransportBuilder->build(new StrategyDtoRedis($recordingRedisClient), new QueueConfig(name: 'q'));
        $transport->receive(1, 0);

        $read = $recordingRedisClient->named('XREADGROUP')[0];
        self::assertSame('consumer-abababababababab', $read[3]);
        self::assertContains('CLAIM', $read, 'level 8.4 detected');
        self::assertSame(['queue' => 'q', 'level' => '8.4'], $recordingLogger->records[0]['context']);
    }

    public function testRedisBuilderHonoursOverrideAndConsumer(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('EVALSHA', 0)->reply('XREADGROUP', reply: null);

        $transport = (new RedisTransportBuilder())->build(
            new StrategyDtoRedis($recordingRedisClient, RedisVersionEnum::GE60, verifyVersion: false, consumer: 'pod-7'),
            new QueueConfig(name: 'q'),
        );
        $transport->receive(1, 0);

        self::assertSame(['EVALSHA', 'XREADGROUP'], $recordingRedisClient->names(), 'no INFO when the override is trusted');
        self::assertSame('pod-7', $recordingRedisClient->named('XREADGROUP')[0][3]);
        self::assertNotContains('CLAIM', $recordingRedisClient->named('XREADGROUP')[0]);
    }

    public function testRedisBuilderRejectsAnOverrideAboveTheServer(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', "redis_version:6.2.14\r\n");

        $this->expectException(VersionMismatchException::class);

        (new RedisTransportBuilder())->build(new StrategyDtoRedis($recordingRedisClient, RedisVersionEnum::GE82), new QueueConfig());
    }

    public function testRedisIdempotencyStoreFactoryPicksTheLockRelease(): void
    {
        $modern = (new RecordingRedisClient())->reply('INFO', "redis_version:8.4.1\r\n");
        $legacy = (new RecordingRedisClient())->reply('INFO', "redis_version:7.2.4\r\n");
        $token = new LockToken(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd'), 't');

        RedisIdempotencyStore::create($modern)->release($token);
        RedisIdempotencyStore::create($legacy, new QueueConfig(name: 'q'), logger: new RecordingLogger())->release($token);
        RedisIdempotencyStore::create(new RecordingRedisClient(), version: RedisVersionEnum::GE84)->release($token);

        self::assertSame('DELEX', $modern->commands[1][0]);
        self::assertSame('EVALSHA', $legacy->commands[1][0]);
        self::assertSame('rq:{q}:idem:0190f2a4-5b6c-7d8e-9f01-23456789abcd', $legacy->commands[1][3]);
    }

    // --- RabbitMQ -------------------------------------------------------------------------------------------

    public function testRabbitMqDtoDefaultsAndValidation(): void
    {
        $fakeAmqpConnector = new FakeAmqpConnector();
        $strategyDtoRabbitMq = new StrategyDtoRabbitMq($fakeAmqpConnector);

        self::assertSame('rabbitmq', $strategyDtoRabbitMq->name());
        self::assertSame([$fakeAmqpConnector, RabbitMqDelayModeEnum::RetryTiers, 10], [$strategyDtoRabbitMq->connector, $strategyDtoRabbitMq->delayMode, $strategyDtoRabbitMq->prefetch]);
        self::assertSame((new RetryTiers())->tiersMs, $strategyDtoRabbitMq->retryTiers->tiersMs);
        self::assertSame(1, (new StrategyDtoRabbitMq($fakeAmqpConnector, prefetch: 1))->prefetch);

        $this->expectException(InvalidArgumentException::class);
        new StrategyDtoRabbitMq($fakeAmqpConnector, prefetch: 0);
    }

    public function testRabbitMqNamedConstructorsCreateAConnector(): void
    {
        $connection = self::createStub(AbstractConnection::class);
        $channel = self::createStub(AMQPChannel::class);
        $channel->method('getConnection')->willReturn($connection);
        $factory = static fn (): AbstractConnection => $connection;

        foreach ([
            StrategyDtoRabbitMq::fromConnection($connection),
            StrategyDtoRabbitMq::fromConnection($connection, publisherConfirms: false),
            StrategyDtoRabbitMq::fromPhpAmqpLib($channel),
            StrategyDtoRabbitMq::fromConnectionFactory($factory),
        ] as $strategyDtoRabbitMq) {
            self::assertInstanceOf(PhpAmqpLibConnector::class, $strategyDtoRabbitMq->connector);
            self::assertSame(0, $strategyDtoRabbitMq->connector->generation(), 'no channel is opened by the DTO');
        }
    }

    public function testFromPhpAmqpLibRequiresAConnection(): void
    {
        $channel = self::createStub(AMQPChannel::class);
        $channel->method('getConnection')->willReturn(null);

        $this->expectException(InvalidArgumentException::class);

        StrategyDtoRabbitMq::fromPhpAmqpLib($channel);
    }

    public function testRabbitMqBuilder(): void
    {
        $fakeAmqpConnector = new FakeAmqpConnector();
        $rabbitMqTransportBuilder = new RabbitMqTransportBuilder($this->fakeClock, new RecordingLogger());

        self::assertTrue($rabbitMqTransportBuilder->supports(new StrategyDtoRabbitMq($fakeAmqpConnector)));
        self::assertFalse($rabbitMqTransportBuilder->supports($this->foreignDto()));

        $transport = $rabbitMqTransportBuilder->build(new StrategyDtoRabbitMq($fakeAmqpConnector, prefetch: 3), new QueueConfig(name: 'q'));
        $transport->receive(1, 0);

        self::assertContains('prefetch:3', $fakeAmqpConnector->channel->calls);
        self::assertContains('consume:rq.q', $fakeAmqpConnector->channel->calls);
        self::assertInstanceOf(RabbitMqTransport::class, (new RabbitMqTransportBuilder())->build(new StrategyDtoRabbitMq($fakeAmqpConnector), new QueueConfig()));
    }

    public function testBuildersRejectForeignDtos(): void
    {
        foreach ([new InMemoryTransportBuilder(), new RedisTransportBuilder(), new RabbitMqTransportBuilder()] as $builder) {
            try {
                $builder->build($this->foreignDto(), new QueueConfig());
                self::fail('Expected UnsupportedStrategyException');
            } catch (UnsupportedStrategyException $unsupportedStrategyException) {
                self::assertStringContainsString($builder::class, $unsupportedStrategyException->getMessage());
            }
        }
    }

    public function testDelexAndLuaReleaseAreTheTwoChoices(): void
    {
        self::assertInstanceOf(DelexLockRelease::class, FeatureSelector::select(RedisCapabilities::forVersion(RedisVersionEnum::GE84))->lockRelease);
        self::assertInstanceOf(LuaLockRelease::class, FeatureSelector::select(RedisCapabilities::forVersion(RedisVersionEnum::GE80))->lockRelease);
    }

    private function foreignDto(): StrategyDtoInterface
    {
        return new readonly class implements StrategyDtoInterface {
            #[Override]
            public function name(): string
            {
                return 'sqs';
            }
        };
    }
}
