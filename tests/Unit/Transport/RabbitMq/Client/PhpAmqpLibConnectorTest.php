<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\RabbitMq\Client;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Tests\Support\CallLog;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibChannel;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use RuntimeException;

#[CoversClass(PhpAmqpLibConnector::class)]
#[UsesClass(PhpAmqpLibChannel::class)]
#[UsesClass(ConnectionException::class)]
#[UsesClass(CommandFailedException::class)]
final class PhpAmqpLibConnectorTest extends TestCase
{
    public function testReusesTheOpenChannelAndReopensAClosedOne(): void
    {
        $first = $this->amqpChannel(open: false);
        $second = $this->amqpChannel(open: true);
        $connection = $this->connection(connected: true);
        $connection->expects(self::exactly(2))->method('channel')->willReturnOnConsecutiveCalls($first, $second);

        $phpAmqpLibConnector = new PhpAmqpLibConnector($connection, publisherConfirms: false);
        self::assertSame(0, $phpAmqpLibConnector->generation());

        $channel = $phpAmqpLibConnector->channel();
        self::assertSame(1, $phpAmqpLibConnector->generation());

        $reopened = $phpAmqpLibConnector->channel();
        self::assertNotSame($channel, $reopened);
        self::assertSame(2, $phpAmqpLibConnector->generation());

        self::assertSame($reopened, $phpAmqpLibConnector->channel());
        self::assertSame(2, $phpAmqpLibConnector->generation());
    }

    public function testResetClosesAndForgetsTheChannel(): void
    {
        $amqpChannel = $this->amqpChannel(open: true);
        $amqpChannel->expects(self::once())->method('close');
        $connection = $this->connection(connected: true);
        $connection->expects(self::exactly(2))->method('channel')->willReturn($amqpChannel);

        $phpAmqpLibConnector = new PhpAmqpLibConnector($connection, publisherConfirms: false);
        $phpAmqpLibConnector->reset();
        $phpAmqpLibConnector->channel();
        $phpAmqpLibConnector->reset();
        $phpAmqpLibConnector->channel();

        self::assertSame(2, $phpAmqpLibConnector->generation());
    }

    public function testEnablesPublisherConfirmsByDefault(): void
    {
        $amqpChannel = $this->amqpChannel(open: true);
        $amqpChannel->expects(self::once())->method('confirm_select');
        $connection = $this->connection(connected: true);
        $connection->method('channel')->willReturn($amqpChannel);

        (new PhpAmqpLibConnector($connection))->channel();
    }

    public function testFactoryCreatesTheConnectionLazilyAndAgainAfterItDrops(): void
    {
        $dropped = $this->connection(connected: false);
        $healthy = $this->connection(connected: true);
        $healthy->method('channel')->willReturn($this->amqpChannel(open: false));
        $callLog = new CallLog();

        $phpAmqpLibConnector = new PhpAmqpLibConnector(static function () use ($callLog, $dropped, $healthy): AbstractConnection {
            $callLog->record('connect');

            return 1 === \count($callLog->calls) ? $dropped : $healthy;
        }, publisherConfirms: false);
        self::assertSame([], $callLog->calls);

        try {
            $phpAmqpLibConnector->channel();
            self::fail('Expected ConnectionException');
        } catch (ConnectionException $connectionException) {
            self::assertStringContainsString('the connection is closed', $connectionException->getMessage());
        }

        $phpAmqpLibConnector->channel();
        $phpAmqpLibConnector->channel();
        self::assertSame(['connect', 'connect'], $callLog->calls);
    }

    public function testFactoryFailureBecomesConnectionException(): void
    {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('RabbitMQ backend is unreachable: refused');

        (new PhpAmqpLibConnector(static fn (): AbstractConnection => throw new RuntimeException('refused')))->channel();
    }

    public function testClosedConnectionWithoutFactory(): void
    {
        $this->expectException(ConnectionException::class);

        (new PhpAmqpLibConnector($this->connection(connected: false)))->channel();
    }

    public function testChannelOpeningFailureBecomesConnectionException(): void
    {
        $connection = $this->connection(connected: true);
        $connection->method('channel')->willThrowException(new RuntimeException('channel_max reached'));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('channel_max reached');

        (new PhpAmqpLibConnector($connection))->channel();
    }

    /**
     * @return AbstractConnection&MockObject
     */
    private function connection(bool $connected): MockObject
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn($connected);

        return $connection;
    }

    /**
     * @return AMQPChannel&MockObject
     */
    private function amqpChannel(bool $open): MockObject
    {
        $amqpChannel = $this->createMock(AMQPChannel::class);
        $amqpChannel->method('is_open')->willReturn($open);

        return $amqpChannel;
    }
}
