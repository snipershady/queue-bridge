<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Integration\RabbitMq;

use Override;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\System\SystemClock;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibChannel;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use QueueBridge\Transport\RabbitMq\RabbitMqTransport;
use QueueBridge\Transport\RabbitMq\RetryTiers;
use QueueBridge\Transport\RabbitMq\Topology;
use Throwable;

/**
 * Topologia (delivery-limit e DLX), scaglioni di retry, riconnessione.
 */
#[Group('rabbitmq')]
#[CoversClass(RabbitMqTransport::class)]
#[CoversClass(Topology::class)]
#[CoversClass(PhpAmqpLibConnector::class)]
#[UsesClass(PhpAmqpLibChannel::class)]
#[UsesClass(RetryTiers::class)]
final class RabbitMqTopologyTest extends TestCase
{
    private SystemClock $systemClock;

    private UuidV7Generator $uuidV7Generator;

    private QueueConfig $queueConfig;

    private bool $reachable = false;

    /** @var list<AbstractConnection> */
    private array $connections = [];

    #[Override]
    protected function setUp(): void
    {
        $this->systemClock = new SystemClock();
        $this->uuidV7Generator = new UuidV7Generator($this->systemClock);
        $this->queueConfig = new QueueConfig(name: 'topology', prefix: 'it'.bin2hex(random_bytes(6)), maxAttempts: 1, maxDeliveries: 2);

        try {
            $this->connection()->close();
        } catch (Throwable $throwable) {
            self::markTestSkipped('RabbitMQ is not reachable: '.$throwable->getMessage());
        }

        $this->reachable = true;
    }

    #[Override]
    protected function tearDown(): void
    {
        if (!$this->reachable) {
            return;
        }

        $connection = $this->connection();
        $channel = $connection->channel();
        $topology = new Topology($this->queueConfig, new RetryTiers());
        foreach ([$topology->queue, $topology->deadLetterQueue, ...$topology->retryQueues()] as $queue) {
            $channel->queue_delete($queue);
        }

        $channel->exchange_delete($topology->exchange);
        foreach ($this->connections as $openConnection) {
            try {
                $openConnection->close();
            } catch (Throwable) {
            }
        }
    }

    public function testBrokerDeliveryLimitDeadLettersAMessageThatKeepsCrashingTheConsumer(): void
    {
        $rabbitMqTransport = $this->transport();
        $rabbitMqTransport->setup();
        $rabbitMqTransport->send($this->message('poison'));

        // maxDeliveries = 2: alla terza restituzione il broker lo sposta in DLQ da solo.
        $deliveryCounts = [];
        for ($i = 0; $i < 4; ++$i) {
            foreach ($rabbitMqTransport->receive(1, 500) as $delivery) {
                $deliveryCounts[] = $delivery->deliveryCount;
            }

            $rabbitMqTransport->close();
        }

        self::assertSame([1, 2, 3], $deliveryCounts);
        $deadLetters = $rabbitMqTransport->deadLetters();
        self::assertCount(1, $deadLetters);
        self::assertSame('poison', $deadLetters[0]->message->text);
    }

    public function testDelayIsHonouredAcrossTiers(): void
    {
        $rabbitMqTransport = $this->transport();
        $rabbitMqTransport->setup();

        $sentAt = hrtime(as_number: true);
        $rabbitMqTransport->send($this->message('later'), 1500);

        $received = [];
        while ([] === $received && hrtime(as_number: true) - $sentAt < 6_000_000_000) {
            $received = $rabbitMqTransport->receive(1, 200);
        }

        $elapsedMs = (hrtime(as_number: true) - $sentAt) / 1_000_000;
        self::assertCount(1, $received);
        self::assertGreaterThanOrEqual(1500, $elapsedMs);
        self::assertLessThan(3500, $elapsedMs);
    }

    public function testRecoversAfterTheBrokerClosesTheChannel(): void
    {
        $rabbitMqTransport = $this->transport();

        // Prima del setup la dichiarazione passiva fallisce e il broker chiude il canale.
        self::assertSame(0, $rabbitMqTransport->stats()->ready);

        $rabbitMqTransport->setup();
        $rabbitMqTransport->send($this->message('after channel error'));

        self::assertSame('after channel error', $rabbitMqTransport->receive(1, 1000)[0]->message->text);
    }

    public function testDeadLetterOperationsBeforeSetupReturnEmptyResults(): void
    {
        $rabbitMqTransport = $this->transport();

        self::assertSame([], $rabbitMqTransport->deadLetters());
        self::assertSame(0, $rabbitMqTransport->requeueDeadLetters());
        self::assertFalse($rabbitMqTransport->requeueDeadLetter($this->message('x')->id));
        self::assertSame(0, $rabbitMqTransport->purgeDeadLetters());

        $rabbitMqTransport->setup();
        $rabbitMqTransport->send($this->message('after setup'));
        self::assertSame('after setup', $rabbitMqTransport->receive(1, 1000)[0]->message->text);
    }

    public function testRejectIsNotAcknowledgedWhenTheBrokerCannotRouteTheCopy(): void
    {
        $rabbitMqTransport = $this->transport();
        $rabbitMqTransport->setup();
        $rabbitMqTransport->send($message = $this->message('must survive'));
        [$delivery] = $rabbitMqTransport->receive(1, 1000);

        // DLQ cancellata da fuori: la copia non è instradabile (basic.return), quindi l'originale non va confermato.
        $connection = $this->connection();
        $connection->channel()->queue_delete((new Topology($this->queueConfig, new RetryTiers()))->deadLetterQueue);
        $connection->close();

        try {
            $rabbitMqTransport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->systemClock->now()));
            self::fail('Expected CommandFailedException');
        } catch (CommandFailedException $commandFailedException) {
            self::assertStringContainsString('was not confirmed', $commandFailedException->getMessage());
        }

        $again = $rabbitMqTransport->receive(1, 2000);
        self::assertCount(1, $again);
        self::assertTrue($again[0]->message->id->equals($message->id));
        self::assertSame(2, $again[0]->deliveryCount);
    }

    public function testReconnectsAfterTheConnectionDrops(): void
    {
        $rabbitMqTransport = $this->transport();
        $rabbitMqTransport->setup();
        $rabbitMqTransport->send($this->message('before'));

        foreach ($this->connections as $connection) {
            $connection->close();
        }

        $rabbitMqTransport->close();
        $rabbitMqTransport->send($this->message('after'));

        $texts = [];
        foreach ([...$rabbitMqTransport->receive(10, 1000), ...$rabbitMqTransport->receive(10, 500)] as $delivery) {
            $texts[] = $delivery->message->text;
        }

        sort($texts);
        self::assertSame(['after', 'before'], $texts);
    }

    private function transport(): RabbitMqTransport
    {
        return new RabbitMqTransport(new PhpAmqpLibConnector(function (): AbstractConnection {
            $connection = $this->connection();
            $this->connections[] = $connection;

            return $connection;
        }), $this->queueConfig, $this->systemClock);
    }

    private function message(string $text): Message
    {
        return new Message($this->uuidV7Generator->generate(), 'test', $text, $this->systemClock->now());
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
            connection_timeout: 2.0,
            read_write_timeout: 10.0,
        );
    }
}
