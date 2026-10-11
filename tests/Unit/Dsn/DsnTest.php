<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Dsn;

use ArrayObject;
use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Predis\ClientInterface;
use Predis\Response\Status;
use QueueBridge\Dsn\Dsn;
use QueueBridge\Dsn\DsnParser;
use QueueBridge\Dsn\StrategyDtoFactory;
use QueueBridge\Dsn\StrategyEnum;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Transport\InMemory\StrategyDtoInMemory;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibChannel;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use QueueBridge\Transport\RabbitMq\RabbitMqDelayModeEnum;
use QueueBridge\Transport\RabbitMq\RetryTiers;
use QueueBridge\Transport\RabbitMq\StrategyDtoRabbitMq;
use QueueBridge\Transport\Redis\Client\PhpRedisClient;
use QueueBridge\Transport\Redis\Client\PredisClient;
use QueueBridge\Transport\Redis\StrategyDtoRedis;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;
use Redis;

#[CoversClass(StrategyEnum::class)]
#[CoversClass(Dsn::class)]
#[CoversClass(DsnParser::class)]
#[CoversClass(StrategyDtoFactory::class)]
#[UsesClass(StrategyDtoInMemory::class)]
#[UsesClass(StrategyDtoRedis::class)]
#[UsesClass(StrategyDtoRabbitMq::class)]
#[UsesClass(PhpRedisClient::class)]
#[UsesClass(PredisClient::class)]
#[UsesClass(PhpAmqpLibConnector::class)]
#[UsesClass(PhpAmqpLibChannel::class)]
#[UsesClass(RetryTiers::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(ConnectionException::class)]
final class DsnTest extends TestCase
{
    /**
     * @return iterable<string, array{string, StrategyEnum}>
     */
    public static function schemes(): iterable
    {
        yield 'redis' => ['redis', StrategyEnum::Redis];
        yield 'rediss' => ['rediss', StrategyEnum::Redis];
        yield 'amqp' => ['AMQP', StrategyEnum::RabbitMq];
        yield 'amqps' => ['amqps', StrategyEnum::RabbitMq];
        yield 'memory' => ['memory', StrategyEnum::InMemory];
    }

    #[DataProvider('schemes')]
    public function testStrategyFromScheme(string $scheme, StrategyEnum $expected): void
    {
        self::assertSame($expected, StrategyEnum::fromScheme($scheme));
    }

    public function testUnknownSchemeIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported DSN scheme "sqs"');

        StrategyEnum::fromScheme('sqs');
    }

    public function testParsesRedisDsn(): void
    {
        $dsn = DsnParser::parse('rediss://app:p%40ss@cache.local:6380/3?version=7.0&verify_version=0');

        self::assertSame(StrategyEnum::Redis, $dsn->strategy);
        self::assertTrue($dsn->secure);
        self::assertSame(['cache.local', 6380, 'app', 'p@ss', '3'], [$dsn->host, $dsn->port, $dsn->user, $dsn->password, $dsn->path]);
        self::assertSame(['version' => '7.0', 'verify_version' => '0'], $dsn->options);
    }

    public function testParsesAmqpDsnWithEncodedVhost(): void
    {
        $dsn = DsnParser::parse('amqp://guest:guest@rabbit/%2F?prefetch=5&skip[]=x');

        self::assertSame(StrategyEnum::RabbitMq, $dsn->strategy);
        self::assertFalse($dsn->secure);
        self::assertNull($dsn->port);
        self::assertSame('/', $dsn->path);
        self::assertSame(['prefetch' => '5'], $dsn->options, 'array options are ignored');
    }

    public function testParsesMinimalDsns(): void
    {
        $redis = DsnParser::parse('redis://:secret@localhost');
        self::assertNull($redis->user);
        self::assertSame('secret', $redis->password);
        self::assertSame('', $redis->path);

        $memory = DsnParser::parse('memory://');
        self::assertSame(StrategyEnum::InMemory, $memory->strategy);
        self::assertSame(['127.0.0.1', null, null, null, '', []], [$memory->host, $memory->port, $memory->user, $memory->password, $memory->path, $memory->options]);
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function invalidDsns(): iterable
    {
        yield 'no scheme' => ['localhost:6379', 'expected scheme://'];
        yield 'empty scheme' => ['://host', 'expected scheme://'];
        yield 'no host' => ['redis://', 'a host is required'];
        yield 'unix socket' => ['redis:///tmp/redis.sock', 'a host is required'];
        yield 'unknown scheme' => ['kafka://broker', 'Unsupported DSN scheme'];
    }

    #[DataProvider('invalidDsns')]
    public function testRejectsInvalidDsns(string $dsn, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        DsnParser::parse($dsn);
    }

    public function testRedactsPasswords(): void
    {
        self::assertSame('amqp://guest:***@host/vh', DsnParser::redact('amqp://guest:secret@host/vh'));
        self::assertSame('redis://:***@host', DsnParser::redact('redis://:secret@host'));
        self::assertSame('redis://host', DsnParser::redact('redis://host'));
    }

    public function testOptionAccessors(): void
    {
        $dsn = new Dsn(StrategyEnum::Redis, secure: false, options: ['n' => '5', 'neg' => '-1', 'f' => '1.5', 'yes' => 'ON', 'no' => 'false', 's' => 'v']);

        self::assertSame('v', $dsn->option('s', 'd'));
        self::assertSame('d', $dsn->option('missing', 'd'));
        self::assertSame(5, $dsn->intOption('n', 1));
        self::assertSame(-1, $dsn->intOption('neg', 1, -5));
        self::assertSame(9, $dsn->intOption('missing', 9));
        self::assertSame(1.5, $dsn->floatOption('f', 2.0));
        self::assertSame(2.0, $dsn->floatOption('missing', 2.0));
        self::assertTrue($dsn->boolOption('yes', default: false));
        self::assertFalse($dsn->boolOption('no', default: true));
        self::assertTrue($dsn->boolOption('missing', default: true));
        self::assertSame(5, $dsn->intOption('n', 1, 5), 'the minimum is inclusive');
    }

    public function testEveryBooleanSpelling(): void
    {
        $dsn = new Dsn(StrategyEnum::Redis, secure: false, options: ['a' => '1', 'b' => 'true', 'c' => 'Yes', 'd' => 'on', 'e' => '0', 'f' => 'False', 'g' => 'no', 'h' => 'OFF']);

        self::assertSame(
            [true, true, true, true, false, false, false, false],
            array_map(static fn (string $name): bool => $dsn->boolOption($name, default: !\in_array($name, ['a', 'b', 'c', 'd'], strict: true)), ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']),
        );
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function malformedIntegers(): iterable
    {
        yield 'leading garbage' => ['x5'];
        yield 'trailing garbage' => ['5x'];
        yield 'trailing newline' => ["5\n"];
    }

    #[DataProvider('malformedIntegers')]
    public function testIntOptionRequiresTheWholeValueToBeAnInteger(string $value): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new Dsn(StrategyEnum::Redis, secure: false, options: ['n' => $value]))->intOption('n', 1);
    }

    public function testSecureFlagFollowsTheScheme(): void
    {
        self::assertTrue(DsnParser::parse('REDISS://h')->secure);
        self::assertTrue(DsnParser::parse('amqps://h')->secure);
        self::assertFalse(DsnParser::parse('redis://h')->secure);
        self::assertFalse(DsnParser::parse('memory://')->secure);
    }

    public function testTiersAreTrimmed(): void
    {
        $dto = (new StrategyDtoFactory())->fromDsn('amqp://h?tiers='.rawurlencode(' 1000 , 5000'));

        self::assertInstanceOf(StrategyDtoRabbitMq::class, $dto);
        self::assertSame([1000, 5000], $dto->retryTiers->tiersMs);
    }

    /**
     * @return iterable<string, array{callable(Dsn): mixed, string}>
     */
    public static function invalidOptions(): iterable
    {
        yield 'int below minimum' => [static fn (Dsn $dsn): int => $dsn->intOption('neg', 0), 'must be an integer >= 0, got "-1"'];
        yield 'int not numeric' => [static fn (Dsn $dsn): int => $dsn->intOption('s', 0), 'must be an integer'];
        yield 'float not numeric' => [static fn (Dsn $dsn): float => $dsn->floatOption('s', 1.0), 'must be a number > 0'];
        yield 'float zero' => [static fn (Dsn $dsn): float => $dsn->floatOption('zero', 1.0), 'must be a number > 0'];
        yield 'bool garbage' => [static fn (Dsn $dsn): bool => $dsn->boolOption('s', default: true), 'must be a boolean'];
    }

    /**
     * @param callable(Dsn): mixed $call
     */
    #[DataProvider('invalidOptions')]
    public function testOptionValidation(callable $call, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        $call(new Dsn(StrategyEnum::Redis, secure: false, options: ['neg' => '-1', 's' => 'abc', 'zero' => '0']));
    }

    // --- StrategyDtoFactory ----------------------------------------------------------------------------------

    public function testMemoryDsn(): void
    {
        self::assertInstanceOf(StrategyDtoInMemory::class, (new StrategyDtoFactory())->fromDsn('memory://'));
    }

    #[RequiresPhpExtension('redis')]
    public function testPhpRedisConnectsLazilyWithTlsAclAndDatabase(): void
    {
        $redis = $this->createMock(Redis::class);
        $redis->expects(self::once())->method('connect')->with('tls://cache.local', 6380, 1.5)->willReturn(true);
        $redis->expects(self::once())->method('auth')->with(['app', 'p@ss'])->willReturn(true);
        $redis->expects(self::once())->method('select')->with(3)->willReturn(true);
        $redis->method('rawcommand')->willReturn(true);
        $created = 0;
        $strategyDtoFactory = new StrategyDtoFactory(newPhpRedis: static function () use ($redis, &$created): Redis {
            ++$created;

            return $redis;
        }, phpRedisAvailable: true);

        $dto = $strategyDtoFactory->fromDsn('rediss://app:p%40ss@cache.local:6380/3?version=7.0&verify_version=0&consumer=pod-1&timeout=1.5');

        self::assertInstanceOf(StrategyDtoRedis::class, $dto);
        self::assertInstanceOf(PhpRedisClient::class, $dto->client);
        self::assertSame([RedisVersionEnum::GE70, false, 'pod-1'], [$dto->version, $dto->verifyVersion, $dto->consumer]);
        self::assertSame(0, $created, 'the connection is lazy');
        self::assertSame('OK', $dto->client->execute(['PING']));
    }

    #[RequiresPhpExtension('redis')]
    public function testPhpRedisWithPasswordOnlyAndDefaultDatabase(): void
    {
        $redis = $this->createMock(Redis::class);
        $redis->expects(self::once())->method('connect')->with('localhost', 6379, 2.0)->willReturn(true);
        $redis->expects(self::once())->method('auth')->with('secret')->willReturn(true);
        $redis->expects(self::never())->method('select');
        $redis->method('rawcommand')->willReturn('PONG');

        $dto = (new StrategyDtoFactory(newPhpRedis: static fn (): Redis => $redis))->fromDsn('redis://:secret@localhost?client=phpredis');
        self::assertInstanceOf(StrategyDtoRedis::class, $dto);

        self::assertSame('PONG', $dto->client->execute(['PING']));
        self::assertNull($dto->version);
        self::assertTrue($dto->verifyVersion);
        self::assertNull($dto->consumer);
    }

    #[RequiresPhpExtension('redis')]
    public function testPhpRedisWithoutCredentials(): void
    {
        $redis = $this->createMock(Redis::class);
        $redis->expects(self::once())->method('connect')->with('h', 6379, 2.0)->willReturn(true);
        $redis->expects(self::never())->method('auth');
        $redis->method('rawcommand')->willReturn('PONG');

        $dto = (new StrategyDtoFactory(newPhpRedis: static fn (): Redis => $redis, phpRedisAvailable: true))->fromDsn('redis://h');
        self::assertInstanceOf(StrategyDtoRedis::class, $dto);
        $dto->client->execute(['PING']);
    }

    public function testPredisIsUsedWithoutExtRedis(): void
    {
        $captured = new ArrayObject();
        $client = self::createStub(ClientInterface::class);
        $client->method('executeCommand')->willReturn(new Status('PONG'));
        $strategyDtoFactory = new StrategyDtoFactory(newPredis: static function (array $parameters, array $options) use ($captured, $client): ClientInterface {
            $captured->exchangeArray([$parameters, $options]);

            return $client;
        }, phpRedisAvailable: false);

        $dto = $strategyDtoFactory->fromDsn('rediss://app:pw@cache:6390/2');

        self::assertInstanceOf(StrategyDtoRedis::class, $dto);
        self::assertInstanceOf(PredisClient::class, $dto->client);
        self::assertSame([
            ['scheme' => 'tls', 'host' => 'cache', 'port' => 6390, 'database' => 2, 'username' => 'app', 'password' => 'pw', 'timeout' => 2.0],
            ['exceptions' => false],
        ], $captured->getArrayCopy());
        self::assertSame('PONG', $dto->client->execute(['PING']));
    }

    public function testPredisDefaults(): void
    {
        $captured = new ArrayObject();
        $strategyDtoFactory = new StrategyDtoFactory(newPredis: static function (array $parameters, array $options) use ($captured): ClientInterface {
            $captured->exchangeArray($parameters);

            return self::createStub(ClientInterface::class);
        }, phpRedisAvailable: true);

        $strategyDtoFactory->fromDsn('redis://cache?client=predis');

        self::assertSame(
            ['scheme' => 'tcp', 'host' => 'cache', 'port' => 6379, 'database' => 0, 'username' => null, 'password' => null, 'timeout' => 2.0],
            $captured->getArrayCopy(),
        );
    }

    public function testDefaultFactoriesCreateRealClients(): void
    {
        $dto = (new StrategyDtoFactory(phpRedisAvailable: false))->fromDsn('redis://127.0.0.1:1');

        self::assertInstanceOf(StrategyDtoRedis::class, $dto);
        self::assertInstanceOf(PredisClient::class, $dto->client);
        self::assertInstanceOf(StrategyDtoRabbitMq::class, (new StrategyDtoFactory())->fromDsn('amqp://127.0.0.1:1'));
    }

    #[RequiresPhpExtension('redis')]
    public function testDefaultPhpRedisFactoryConnects(): void
    {
        $dto = (new StrategyDtoFactory(phpRedisAvailable: true))->fromDsn('redis://127.0.0.1:1?timeout=0.2');
        self::assertInstanceOf(StrategyDtoRedis::class, $dto);

        $this->expectException(ConnectionException::class);

        $dto->client->execute(['PING']);
    }

    public function testDefaultAmqpFactoryConnects(): void
    {
        $dto = (new StrategyDtoFactory())->fromDsn('amqp://127.0.0.1:1?timeout=0.2');
        self::assertInstanceOf(StrategyDtoRabbitMq::class, $dto);

        $this->expectException(ConnectionException::class);

        $dto->connector->channel();
    }

    public function testAmqpConfigurationIsLazyAndComplete(): void
    {
        $captured = new ArrayObject();
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $connection->method('channel')->willReturn(self::createStub(AMQPChannel::class));
        $strategyDtoFactory = new StrategyDtoFactory(newAmqp: static function (AMQPConnectionConfig $amqpConnectionConfig) use ($captured, $connection): AbstractConnection {
            $captured->exchangeArray([$amqpConnectionConfig]);

            return $connection;
        });

        $dto = $strategyDtoFactory->fromDsn('amqps://app:pw@rabbit/orders?prefetch=25&confirms=0&heartbeat=30&timeout=1.5&tiers=500,%202000');

        self::assertInstanceOf(StrategyDtoRabbitMq::class, $dto);
        self::assertCount(0, $captured, 'the connection is lazy');
        self::assertSame([25, RabbitMqDelayModeEnum::RetryTiers, [500, 2000]], [$dto->prefetch, $dto->delayMode, $dto->retryTiers->tiersMs]);

        $dto->connector->channel();

        $captured = $captured->getArrayCopy()[0] ?? null;
        self::assertInstanceOf(AMQPConnectionConfig::class, $captured);
        self::assertSame(
            ['rabbit', 5671, 'app', 'pw', 'orders', true, 30, 1.5],
            [$captured->getHost(), $captured->getPort(), $captured->getUser(), $captured->getPassword(), $captured->getVhost(), $captured->isSecure(), $captured->getHeartbeat(), $captured->getConnectionTimeout()],
        );
    }

    public function testAmqpDefaults(): void
    {
        $captured = new ArrayObject();
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $channel = $this->createMock(AMQPChannel::class);
        $channel->expects(self::once())->method('confirm_select');
        $connection->method('channel')->willReturn($channel);

        $dto = (new StrategyDtoFactory(newAmqp: static function (AMQPConnectionConfig $amqpConnectionConfig) use ($captured, $connection): AbstractConnection {
            $captured->exchangeArray([$amqpConnectionConfig]);

            return $connection;
        }))->fromDsn('amqp://rabbit');
        self::assertInstanceOf(StrategyDtoRabbitMq::class, $dto);
        $dto->connector->channel();

        $captured = $captured->getArrayCopy()[0] ?? null;
        self::assertInstanceOf(AMQPConnectionConfig::class, $captured);
        self::assertSame(['rabbit', 5672, 'guest', 'guest', '/', false, 0, 3.0], [$captured->getHost(), $captured->getPort(), $captured->getUser(), $captured->getPassword(), $captured->getVhost(), $captured->isSecure(), $captured->getHeartbeat(), $captured->getConnectionTimeout()]);
        self::assertSame(10, $dto->prefetch);
        self::assertSame((new RetryTiers())->tiersMs, $dto->retryTiers->tiersMs);
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function invalidFactoryDsns(): iterable
    {
        yield 'unknown version' => ['redis://h?version=9.9', 'must be one of 6.0, 6.2, 7.0, 8.0, 8.2, 8.4'];
        yield 'non numeric database' => ['redis://h/cache', 'Invalid Redis database "cache"'];
        yield 'unknown client' => ['redis://h?client=jedis', 'must be "phpredis" or "predis"'];
        yield 'invalid tiers' => ['amqp://h?tiers=1s', 'must list milliseconds'];
        yield 'tiers with leading garbage' => ['amqp://h?tiers=x1', 'must list milliseconds'];
        yield 'database with trailing newline' => ['redis://h/1%0A', 'Invalid Redis database'];
        yield 'zero prefetch' => ['amqp://h?prefetch=0', 'must be an integer >= 1'];
    }

    #[DataProvider('invalidFactoryDsns')]
    public function testFactoryRejectsInvalidOptions(string $dsn, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        (new StrategyDtoFactory(phpRedisAvailable: true))->fromDsn($dsn);
    }
}
