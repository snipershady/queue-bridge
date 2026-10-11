<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Integration\RabbitMq;

use Override;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use Psr\Clock\ClockInterface;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Tests\Contract\TransportContractTestCase;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibChannel;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use snipershady\Queue\Transport\RabbitMq\RabbitMqTransport;
use snipershady\Queue\Transport\RabbitMq\RetryTiers;
use snipershady\Queue\Transport\RabbitMq\Topology;
use snipershady\Queue\Transport\TransportInterface;
use Throwable;

/**
 * Contract test su un RabbitMQ reale (AMQP_DSN). Ogni test usa un prefisso casuale e cancella le proprie code.
 */
#[Group('rabbitmq')]
#[CoversClass(RabbitMqTransport::class)]
#[CoversClass(PhpAmqpLibConnector::class)]
#[CoversClass(PhpAmqpLibChannel::class)]
#[UsesClass(Topology::class)]
#[UsesClass(RetryTiers::class)]
final class RabbitMqTransportTest extends TransportContractTestCase
{
    private SystemClock $systemClock;

    private ?QueueConfig $queueConfig = null;

    /** @var list<RabbitMqTransport> */
    private array $transports = [];

    #[Override]
    protected function setUp(): void
    {
        $this->systemClock = new SystemClock();

        try {
            $this->connection()->close();
        } catch (Throwable $throwable) {
            self::markTestSkipped('RabbitMQ is not reachable: '.$throwable->getMessage());
        }
    }

    #[Override]
    protected function tearDown(): void
    {
        foreach ($this->transports as $transport) {
            $transport->close();
        }

        if (!$this->queueConfig instanceof QueueConfig) {
            return;
        }

        $connection = $this->connection();
        $channel = $connection->channel();
        $topology = new Topology($this->queueConfig, new RetryTiers());
        foreach ([$topology->queue, $topology->deadLetterQueue, ...$topology->retryQueues()] as $queue) {
            $channel->queue_delete($queue);
        }

        $channel->exchange_delete($topology->exchange);
        $connection->close();
    }

    #[Override]
    protected function createTransport(QueueConfig $queueConfig): TransportInterface
    {
        $this->queueConfig = new QueueConfig(
            name: $queueConfig->name,
            prefix: 'it'.bin2hex(random_bytes(6)),
            maxAttempts: $queueConfig->maxAttempts,
            maxDeliveries: $queueConfig->maxDeliveries,
            claimIdleMs: $queueConfig->claimIdleMs,
            dlqMaxLength: $queueConfig->dlqMaxLength,
        );

        $rabbitMqTransport = $this->transportFor($this->queueConfig);
        $rabbitMqTransport->setup();

        return $rabbitMqTransport;
    }

    #[Override]
    protected function createConsumer(TransportInterface $transport): TransportInterface
    {
        self::assertInstanceOf(QueueConfig::class, $this->queueConfig);

        return $this->transportFor($this->queueConfig);
    }

    #[Override]
    protected function crash(TransportInterface $transport): void
    {
        self::assertInstanceOf(RabbitMqTransport::class, $transport);
        $transport->close();
    }

    #[Override]
    protected function browsesInFlightAndDelayed(): bool
    {
        return false;
    }

    #[Override]
    protected function advanceTimeMs(int $milliseconds): void
    {
        usleep(($milliseconds + 50) * 1000);
    }

    #[Override]
    protected function clock(): ClockInterface
    {
        return $this->systemClock;
    }

    private function transportFor(QueueConfig $queueConfig): RabbitMqTransport
    {
        $rabbitMqTransport = new RabbitMqTransport(new PhpAmqpLibConnector($this->connection(...)), $queueConfig, $this->systemClock);
        $this->transports[] = $rabbitMqTransport;

        return $rabbitMqTransport;
    }

    private function connection(): AMQPStreamConnection
    {
        $dsn = getenv('AMQP_DSN');
        $parts = parse_url(\is_string($dsn) && '' !== $dsn ? $dsn : 'amqp://guest:guest@127.0.0.1:5672');

        return new AMQPStreamConnection(
            $parts['host'] ?? '127.0.0.1',
            $parts['port'] ?? 5672,
            $parts['user'] ?? 'guest',
            $parts['pass'] ?? 'guest',
            isset($parts['path']) && '' !== \trim($parts['path'], '/') ? urldecode(\trim($parts['path'], '/')) : '/',
            connection_timeout: 2.0,
            read_write_timeout: 10.0,
        );
    }
}
