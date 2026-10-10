<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Integration\EndToEnd;

use Override;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
use Redis;
use RedisException;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Exception\UnrecoverableMessageException;
use snipershady\Queue\Factory\QueueFactory;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Queue;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\Retry\FixedDelay;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Transport\RabbitMq\RetryTiers;
use snipershady\Queue\Transport\RabbitMq\StrategyDtoRabbitMq;
use snipershady\Queue\Transport\RabbitMq\Topology;
use snipershady\Queue\Transport\Redis\StrategyDtoRedis;
use snipershady\Queue\Worker\Worker;
use snipershady\Queue\Worker\WorkerConfig;
use snipershady\Queue\Worker\WorkerExitReason;
use Throwable;

/**
 * Lo stesso scenario su ogni backend: cambia soltanto il DTO passato a create() (CLAUDE.md §1, §6, DoD).
 */
#[CoversClass(QueueBridge::class)]
#[CoversClass(QueueFactory::class)]
#[CoversClass(Queue::class)]
#[CoversClass(Worker::class)]
final class SwitchBackendTest extends TestCase
{
    private QueueConfig $queueConfig;

    /** @var list<callable(): void> */
    private array $cleanups = [];

    #[Override]
    protected function setUp(): void
    {
        $this->queueConfig = new QueueConfig(name: 'e2e', prefix: 'it'.bin2hex(random_bytes(6)), maxAttempts: 3);
    }

    #[Override]
    protected function tearDown(): void
    {
        foreach ($this->cleanups as $cleanup) {
            $cleanup();
        }
    }

    public function testInMemory(): void
    {
        $this->scenario(new StrategyDtoInMemory());
    }

    #[Group('redis')]
    public function testRedis(): void
    {
        $dsn = parse_url($this->env('REDIS_DSN', 'redis://127.0.0.1:6379/15'));
        $host = $dsn['host'] ?? '127.0.0.1';
        $port = $dsn['port'] ?? 6379;
        $database = isset($dsn['path']) && '' !== trim($dsn['path'], '/') ? (int) trim($dsn['path'], '/') : 15;

        try {
            $redis = new Redis();
            $redis->connect($host, $port, 1.0);
            $redis->select($database);
        } catch (RedisException $redisException) {
            self::markTestSkipped('Redis is not reachable: '.$redisException->getMessage());
        }

        $prefix = $this->queueConfig->prefix;
        $this->cleanups[] = static function () use ($redis, $prefix): void {
            foreach (['stream', 'delayed', 'dlq'] as $suffix) {
                $redis->del(\sprintf('%s:{e2e}:%s', $prefix, $suffix));
            }
        };

        $this->scenario(StrategyDtoRedis::fromPhpRedis($redis));
    }

    #[Group('rabbitmq')]
    public function testRabbitMq(): void
    {
        $dsn = parse_url($this->env('AMQP_DSN', 'amqp://guest:guest@127.0.0.1:5672'));
        $connect = static fn (): AbstractConnection => new AMQPStreamConnection(
            $dsn['host'] ?? '127.0.0.1',
            $dsn['port'] ?? 5672,
            $dsn['user'] ?? 'guest',
            $dsn['pass'] ?? 'guest',
            connection_timeout: 2.0,
            read_write_timeout: 10.0,
        );

        try {
            $connect()->close();
        } catch (Throwable $throwable) {
            self::markTestSkipped('RabbitMQ is not reachable: '.$throwable->getMessage());
        }

        $queueConfig = $this->queueConfig;
        $this->cleanups[] = static function () use ($connect, $queueConfig): void {
            $connection = $connect();
            $channel = $connection->channel();
            $topology = new Topology($queueConfig, new RetryTiers());
            foreach ([$topology->queue, $topology->deadLetterQueue, ...$topology->retryQueues()] as $queue) {
                $channel->queue_delete($queue);
            }

            $channel->exchange_delete($topology->exchange);
            $connection->close();
        };

        $this->scenario(StrategyDtoRabbitMq::fromConnectionFactory($connect));
    }

    private function scenario(StrategyDtoInterface $strategyDto): void
    {
        // L'unica riga che dipende dal backend è il DTO.
        $queue = QueueBridge::factory()->create($strategyDto, $this->queueConfig);
        $queue->setup();
        $queue->ping();

        $producer = $queue->producer();
        $producer->publish('orders', '{"id":1}');
        $producer->publish('orders', '{"id":2,"fail":"once"}');
        $producer->publish('orders', '{"id":3,"fail":"always"}');
        $producer->publish('audit', 'some json, some data, some text');

        $handled = [];
        $worker = $queue->worker(new WorkerConfig(receiveTimeoutMs: 500, stopWhenEmpty: true, maintenanceIntervalMs: 0, retryStrategy: new FixedDelay(0)))
            ->on('orders', static function (Message $message, DecodedText $decodedText) use (&$handled): void {
                self::assertTrue($decodedText->isJson());
                $order = \is_array($decodedText->value) ? $decodedText->value : [];
                $fail = $order['fail'] ?? null;

                if ('always' === $fail) {
                    throw new UnrecoverableMessageException('invalid order');
                }

                if ('once' === $fail && 0 === $message->attempts) {
                    throw new RetryableException('flaky dependency');
                }

                $handled[] = \sprintf('order %d (attempts %d)', \is_int($order['id'] ?? null) ? $order['id'] : -1, $message->attempts);
            })
            ->on('audit', static function (Message $message, DecodedText $decodedText) use (&$handled): void {
                self::assertFalse($decodedText->isJson());
                $handled[] = 'audit: '.$decodedText->raw;
            });

        self::assertSame(WorkerExitReason::QueueEmpty, $worker->run());

        sort($handled);
        self::assertSame(['audit: some json, some data, some text', 'order 1 (attempts 0)', 'order 2 (attempts 1)'], $handled);

        $deadLetters = $queue->deadLetters();
        self::assertNotNull($deadLetters);
        $dead = $deadLetters->deadLetters();
        self::assertCount(1, $dead);
        self::assertSame(FailureKind::Unrecoverable, $dead[0]->reason->kind);
        self::assertSame('invalid order', $dead[0]->reason->errorMessage);
        self::assertSame('{"id":3,"fail":"always"}', $dead[0]->message->text);

        self::assertSame(1, $deadLetters->requeueDeadLetters());
        $redelivered = $queue->transport()->receive(1, 1000);
        self::assertCount(1, $redelivered);
        self::assertSame(0, $redelivered[0]->message->attempts);
        $queue->transport()->ack($redelivered[0]);

        self::assertSame(0, $queue->stats()?->deadLetters);
    }

    private function env(string $name, string $default): string
    {
        $value = getenv($name);

        return \is_string($value) && '' !== $value ? $value : $default;
    }
}
