<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Factory\QueueFactory;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Message\OutgoingMessage;
use QueueBridge\Producer\Producer;
use QueueBridge\Queue;
use QueueBridge\QueueBridge;
use QueueBridge\System\NativeMemoryMeter;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\NullSignalHandler;
use QueueBridge\System\PcntlSignalHandler;
use QueueBridge\System\SystemClock;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Transport\InMemory\InMemoryBroker;
use QueueBridge\Transport\InMemory\InMemoryEntry;
use QueueBridge\Transport\InMemory\InMemoryTransport;
use QueueBridge\Transport\InMemory\InMemoryTransportBuilder;
use QueueBridge\Transport\InMemory\StrategyDtoInMemory;
use QueueBridge\Transport\RabbitMq\RabbitMqTransportBuilder;
use QueueBridge\Transport\Redis\RedisTransportBuilder;
use ReflectionProperty;

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
