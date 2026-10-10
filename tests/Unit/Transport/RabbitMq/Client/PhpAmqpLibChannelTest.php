<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Transport\RabbitMq\Client;

use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage as NativeMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpMessage;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibChannel;

#[CoversClass(PhpAmqpLibChannel::class)]
#[UsesClass(AmqpMessage::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(ConnectionException::class)]
final class PhpAmqpLibChannelTest extends TestCase
{
    public function testPublisherConfirmsCollectNackedAndReturnedMessages(): void
    {
        $channel = $this->channel();
        $handlers = [];
        $channel->expects(self::once())->method('confirm_select');
        $channel->method('set_nack_handler')->willReturnCallback(static function (Closure $handler) use (&$handlers): void {
            $handlers['nack'] = $handler;
        });
        $channel->method('set_return_listener')->willReturnCallback(static function (Closure $handler) use (&$handlers): void {
            $handlers['return'] = $handler;
        });
        $channel->expects(self::exactly(2))->method('wait_for_pending_acks_returns')->with(2.5);

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel);
        self::assertArrayHasKey('nack', $handlers);
        self::assertArrayHasKey('return', $handlers);
        $handlers['nack'](new NativeMessage('a', ['message_id' => 'id-1']));
        $handlers['return'](312, 'NO_ROUTE', 'ex', 'rk', new NativeMessage('b', ['message_id' => 'id-2']));
        $handlers['nack'](new NativeMessage('c'));

        self::assertSame(['id-1', 'id-2', ''], $phpAmqpLibChannel->confirm(2500));
        self::assertSame([], $phpAmqpLibChannel->confirm(2500));
    }

    public function testWithoutConfirmsConfirmDoesNotWait(): void
    {
        $channel = $this->channel();
        $channel->expects(self::never())->method('confirm_select');
        $channel->expects(self::never())->method('wait_for_pending_acks_returns');

        self::assertSame([], (new PhpAmqpLibChannel($channel, publisherConfirms: false))->confirm(100));
    }

    public function testDeclarationsAndBindings(): void
    {
        $channel = $this->channel();
        $channel->expects(self::once())->method('exchange_declare')
            ->with('ex', 'topic', false, true, false, false, false, self::callback(static fn (AMQPTable $amqpTable): bool => ['a' => 1] === $amqpTable->getNativeData()));
        $channel->expects(self::exactly(2))->method('queue_declare')
            ->willReturnOnConsecutiveCalls(['q', 4, 2], null);
        $channel->expects(self::once())->method('queue_bind')->with('q', 'ex', '#');
        $channel->expects(self::once())->method('basic_qos')->with(0, 7, false);

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);
        $phpAmqpLibChannel->declareExchange('ex', 'topic', ['a' => 1]);
        self::assertSame([4, 2], $phpAmqpLibChannel->declareQueue('q', ['x-queue-type' => 'quorum']));
        self::assertSame([0, 0], $phpAmqpLibChannel->declareQueue('q', passive: true));
        $phpAmqpLibChannel->bindQueue('q', 'ex', '#');
        $phpAmqpLibChannel->prefetch(7);
    }

    public function testDeclareQueueIgnoresNonIntegerCounters(): void
    {
        $channel = $this->channel();
        $channel->method('queue_declare')->willReturn(['q', '4', '2']);

        self::assertSame([0, 0], (new PhpAmqpLibChannel($channel, publisherConfirms: false))->declareQueue('q'));
    }

    public function testQueuesAreDurableNonExclusiveAndNeverAutoDeleted(): void
    {
        $channel = $this->channel();
        $channel->expects(self::once())->method('queue_declare')
            ->with('q', false, true, false, false, false, self::isInstanceOf(AMQPTable::class))
            ->willReturn(['q', 0, 0]);

        (new PhpAmqpLibChannel($channel, publisherConfirms: false))->declareQueue('q');
    }

    public function testConsumerRequiresExplicitAcknowledgements(): void
    {
        $channel = $this->channel();
        $channel->expects(self::once())->method('basic_consume')
            ->with('q', '', false, false, false, false, self::isInstanceOf(Closure::class))
            ->willReturn('ctag');

        (new PhpAmqpLibChannel($channel, publisherConfirms: false))->consume('q', static function (AmqpMessage $amqpMessage): void {
        });
    }

    public function testWaitArguments(): void
    {
        $channel = $this->channel();
        $channel->expects(self::exactly(2))->method('wait')
            ->willReturnCallback(static function (?array $allowed, bool $nonBlocking, float $timeout): null {
                self::assertNull($allowed);
                self::assertSame($nonBlocking ? 0.0 : 0.25, $timeout);

                return null;
            });

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);
        $phpAmqpLibChannel->wait(0);
        $phpAmqpLibChannel->wait(250);
    }

    public function testPurge(): void
    {
        $channel = $this->channel();
        $channel->method('queue_purge')->willReturnOnConsecutiveCalls(5, null);

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);

        self::assertSame(5, $phpAmqpLibChannel->purgeQueue('dlq'));
        self::assertSame(0, $phpAmqpLibChannel->purgeQueue('dlq'));
    }

    public function testConsumeConvertsDeliveries(): void
    {
        $channel = $this->channel();
        $received = [];
        $channel->expects(self::once())->method('basic_consume')->willReturnCallback(static function (string $queue, string $tag, bool $noLocal, bool $noAck, bool $exclusive, bool $nowait, Closure $callback): string {
            $nativeMessage = new NativeMessage('body', ['message_id' => 'm-1', 'application_headers' => new AMQPTable(['x-qb-v' => '1', 'x-delivery-count' => 2, 'x-death' => [['count' => 1]]])]);
            $nativeMessage->setDeliveryTag(9);
            $callback($nativeMessage);
            $callback(new NativeMessage('no headers'));
            $callback(new NativeMessage('numeric id', ['message_id' => 123]));

            return 'ctag';
        });

        (new PhpAmqpLibChannel($channel, publisherConfirms: false))->consume('q', static function (AmqpMessage $amqpMessage) use (&$received): void {
            $received[] = $amqpMessage;
        });

        self::assertCount(3, $received);
        self::assertSame('', $received[2]->messageId);
        self::assertSame(['body', ['x-qb-v' => '1', 'x-delivery-count' => 2], 'm-1', 9], [$received[0]->body, $received[0]->headers, $received[0]->messageId, $received[0]->deliveryTag]);
        self::assertSame([[], '', null], [$received[1]->headers, $received[1]->messageId, $received[1]->deliveryTag]);
    }

    public function testWaitReportsTimeoutsAndBlocksOnlyWhenAsked(): void
    {
        $channel = $this->channel();
        $channel->expects(self::exactly(3))->method('wait')->willReturnCallback(static function (?array $allowed, bool $nonBlocking, float $timeout): null {
            if (!$nonBlocking && 0.5 === $timeout) {
                throw new AMQPTimeoutException('timed out');
            }

            return null;
        });

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);

        self::assertFalse($phpAmqpLibChannel->wait(500));
        self::assertTrue($phpAmqpLibChannel->wait(0));
        self::assertTrue($phpAmqpLibChannel->wait(1000));
    }

    public function testWaitPropagatesConnectionErrors(): void
    {
        $channel = $this->channel();
        $channel->method('wait')->willThrowException(new AMQPConnectionClosedException('closed'));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('RabbitMQ backend is unreachable: closed');

        (new PhpAmqpLibChannel($channel, publisherConfirms: false))->wait(100);
    }

    public function testGet(): void
    {
        $channel = $this->channel();
        $nativeMessage = new NativeMessage('x');
        $nativeMessage->setDeliveryTag(3);
        $channel->method('basic_get')->with('dlq')->willReturnOnConsecutiveCalls($nativeMessage, null);

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);

        self::assertSame(3, $phpAmqpLibChannel->get('dlq')?->deliveryTag);
        self::assertNull($phpAmqpLibChannel->get('dlq'));
    }

    public function testPublishSendsPersistentMandatoryMessages(): void
    {
        $channel = $this->channel();
        $published = [];
        $channel->expects(self::exactly(2))->method('basic_publish')->willReturnCallback(static function (NativeMessage $nativeMessage, string $exchange, string $routingKey, bool $mandatory) use (&$published): null {
            $published[] = [$nativeMessage, $exchange, $routingKey, $mandatory];

            return null;
        });

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);
        $phpAmqpLibChannel->publish(new AmqpMessage('body', ['x-qb-v' => '1'], 'm-1'), 'ex', 'rk');
        $phpAmqpLibChannel->publish(new AmqpMessage('anonymous'), '', 'q');

        [$first, $exchange, $routingKey, $mandatory] = $published[0];
        self::assertSame(['body', 'ex', 'rk', true], [$first->getBody(), $exchange, $routingKey, $mandatory]);
        self::assertSame(NativeMessage::DELIVERY_MODE_PERSISTENT, $first->get('delivery_mode'));
        self::assertSame('m-1', $first->get('message_id'));
        $table = $first->get('application_headers');
        self::assertInstanceOf(AMQPTable::class, $table);
        self::assertSame(['x-qb-v' => '1'], $table->getNativeData());
        self::assertFalse($published[1][0]->has('message_id'));
    }

    public function testAckNackOpenClose(): void
    {
        $channel = $this->channel();
        $channel->expects(self::once())->method('basic_ack')->with(4);
        $channel->expects(self::once())->method('basic_nack')->with(5, false, true);
        $channel->method('is_open')->willReturn(true);
        $channel->expects(self::exactly(2))->method('close')->willReturnOnConsecutiveCalls(null, self::throwException(new RuntimeException('already closed')));

        $phpAmqpLibChannel = new PhpAmqpLibChannel($channel, publisherConfirms: false);
        $phpAmqpLibChannel->ack(4);
        $phpAmqpLibChannel->nack(5, requeue: true);

        self::assertTrue($phpAmqpLibChannel->isOpen());
        $phpAmqpLibChannel->close();
        $phpAmqpLibChannel->close();
    }

    public function testProtocolErrorsBecomeCommandFailedException(): void
    {
        $channel = $this->channel();
        $channel->method('queue_declare')->willThrowException(new AMQPProtocolChannelException(404, "NOT_FOUND - no queue 'q'", [50, 10]));

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage("queue.declare failed: NOT_FOUND - no queue 'q'");

        (new PhpAmqpLibChannel($channel, publisherConfirms: false))->declareQueue('q', passive: true);
    }

    public function testOtherErrorsBecomeConnectionException(): void
    {
        $channel = $this->channel();
        $channel->method('confirm_select')->willThrowException(new AMQPConnectionClosedException('gone'));

        $this->expectException(ConnectionException::class);

        new PhpAmqpLibChannel($channel);
    }

    /**
     * @return AMQPChannel&MockObject
     */
    private function channel(): MockObject
    {
        return $this->createMock(AMQPChannel::class);
    }
}
