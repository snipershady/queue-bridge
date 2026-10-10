<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Factory\QueueFactory;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\Envelope;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Message\OutgoingMessage;
use snipershady\Queue\Producer\Producer;
use snipershady\Queue\Queue;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\PcntlSignalHandler;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\System\UuidV7Generator;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\RecordingLogger;
use snipershady\Queue\Transport\InMemory\InMemoryBroker;
use snipershady\Queue\Transport\InMemory\InMemoryEntry;
use snipershady\Queue\Transport\InMemory\InMemoryTransport;
use snipershady\Queue\Transport\InMemory\InMemoryTransportBuilder;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Transport\RabbitMq\RabbitMqTransportBuilder;
use snipershady\Queue\Transport\Redis\RedisTransportBuilder;

#[CoversClass(QueueBridge::class)]
#[UsesClass(QueueFactory::class)]
#[UsesClass(Queue::class)]
#[UsesClass(InMemoryTransportBuilder::class)]
#[UsesClass(RedisTransportBuilder::class)]
#[UsesClass(RabbitMqTransportBuilder::class)]
#[UsesClass(StrategyDtoInMemory::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(InMemoryEntry::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Producer::class)]
#[UsesClass(UuidV7Generator::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(OutgoingMessage::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(NativeMemoryMeter::class)]
#[UsesClass(NullSignalHandler::class)]
#[UsesClass(PcntlSignalHandler::class)]
final class QueueBridgeTest extends TestCase
{
    public function testFactoryKnowsTheProvidedBackends(): void
    {
        $queueFactory = QueueBridge::factory(new RecordingLogger(), new FakeClock());

        $builders = $this->property($queueFactory, 'builders');
        self::assertIsArray($builders);
        self::assertSame(
            [InMemoryTransportBuilder::class, RedisTransportBuilder::class, RabbitMqTransportBuilder::class],
            array_map(static fn (mixed $builder): string => \is_object($builder) ? $builder::class : '', $builders),
        );

        $queue = $queueFactory->create(new StrategyDtoInMemory(), new QueueConfig(name: 'demo'));
        $queue->producer()->publish('test', 'hello');

        self::assertSame('hello', $queue->transport()->receive()[0]->message->text);
    }

    public function testSignalHandlingFollowsThePcntlAvailability(): void
    {
        self::assertInstanceOf(PcntlSignalHandler::class, $this->property(QueueBridge::factory(signals: true), 'signalHandler'));
        self::assertInstanceOf(NullSignalHandler::class, $this->property(QueueBridge::factory(signals: false), 'signalHandler'));
        self::assertInstanceOf(
            \extension_loaded('pcntl') ? PcntlSignalHandler::class : NullSignalHandler::class,
            $this->property(QueueBridge::factory(), 'signalHandler'),
        );
    }

    private function property(QueueFactory $queueFactory, string $name): mixed
    {
        return (new ReflectionProperty($queueFactory, $name))->getValue($queueFactory);
    }
}
