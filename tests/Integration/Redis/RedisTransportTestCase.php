<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\DataProvider;
use Psr\Clock\ClockInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\System\SystemClock;
use QueueBridge\Tests\Contract\TransportContractTestCase;
use QueueBridge\Transport\Redis\Client\PhpRedisClient;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\RedisKeys;
use QueueBridge\Transport\Redis\RedisReply;
use QueueBridge\Transport\Redis\RedisTransport;
use QueueBridge\Transport\Redis\Version\RedisCapabilities;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;
use QueueBridge\Transport\Redis\Version\VersionDetector;
use QueueBridge\Transport\TransportInterface;
use Redis;
use RedisException;

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

    /**
     * Un membro corrotto dei differiti (scritto da fuori) non deve bloccare la promozione, che gira a ogni receive().
     *
     * @return iterable<string, array{string}>
     */
    public static function corruptDelayedMembers(): iterable
    {
        yield 'not json' => ['not json'];
        yield 'json array' => ['["v","1"]'];
        yield 'nested value' => ['{"v":{"x":"1"}}'];
        yield 'number value' => ['{"v":1}'];
        yield 'empty object' => ['{}'];
        yield 'json scalar' => ['"text"'];
    }

    #[DataProvider('corruptDelayedMembers')]
    public function testCorruptDelayedMemberGoesToTheDeadLetterQueueWithoutBlockingTheQueue(string $member): void
    {
        $transport = $this->createTransport($this->queueConfig());
        self::assertInstanceOf(RedisTransport::class, $transport);
        $keys = $this->keys();
        $this->client()->execute(['ZADD', $keys->delayed(), '1', $member]);
        $transport->send($message = $this->message('test', 'valid'));

        $deliveries = $transport->receive(10, 200);

        self::assertSame(['valid'], $this->texts($deliveries));
        self::assertTrue($deliveries[0]->message->id->equals($message->id));
        self::assertSame(0, RedisReply::int($this->client()->execute(['ZCARD', $keys->delayed()]), 'ZCARD'));
        self::assertSame(1, $transport->stats()->deadLetters, 'the corrupt member is kept for inspection');
        self::assertSame([], $transport->deadLetters(), 'but it is not a valid dead letter');
    }

    public function testRequeueDeadLettersSkipsEntriesWithoutMessageFields(): void
    {
        $transport = $this->createTransport($this->queueConfig());
        self::assertInstanceOf(RedisTransport::class, $transport);
        $keys = $this->keys();
        $this->client()->execute(['XADD', $keys->deadLetters(), '*', 'failure_kind', 'serialization', 'deliveries', '3']);
        $message = $this->message('test', 'requeue me');
        $transport->send($message);
        [$delivery] = $transport->receive(1, 200);
        $transport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->clock()->now()));

        self::assertSame(1, $transport->requeueDeadLetters());
        self::assertSame(0, $transport->stats()->deadLetters, 'the empty entry is dropped');
        self::assertSame(['requeue me'], $this->texts($this->drain($transport)));
    }

    abstract protected function level(): RedisVersionEnum;

    #[Override]
    protected function deadLetterRetentionIsExact(): bool
    {
        return false;
    }

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

    private function client(): RedisClientInterface
    {
        self::assertInstanceOf(RedisClientInterface::class, $this->redisClient);

        return $this->redisClient;
    }

    private function keys(): RedisKeys
    {
        self::assertInstanceOf(QueueConfig::class, $this->queueConfig);

        return new RedisKeys($this->queueConfig);
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
