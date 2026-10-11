<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Factory;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Message\OutgoingMessage;
use QueueBridge\Producer\Producer;
use QueueBridge\Queue;
use QueueBridge\Retry\ExponentialBackoff;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\FakeMemoryMeter;
use QueueBridge\Tests\Support\FakeSignalHandler;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Tests\Support\RecordingTransport;
use QueueBridge\Text\DecodedText;
use QueueBridge\Text\TextDecoder;
use QueueBridge\Transport\Capability\SetupAwareInterface;
use QueueBridge\Transport\InMemory\InMemoryBroker;
use QueueBridge\Transport\InMemory\InMemoryEntry;
use QueueBridge\Transport\InMemory\InMemoryTransport;
use QueueBridge\Transport\MaintenanceReport;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;
use QueueBridge\Worker\CallableHandler;
use QueueBridge\Worker\Event\MessageHandled;
use QueueBridge\Worker\Event\MessageReceived;
use QueueBridge\Worker\Event\WorkerStopped;
use QueueBridge\Worker\HandlerRegistry;
use QueueBridge\Worker\StopConditions;
use QueueBridge\Worker\Worker;
use QueueBridge\Worker\WorkerConfig;
use QueueBridge\Worker\WorkerExitReason;

#[CoversClass(Queue::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Producer::class)]
#[UsesClass(Worker::class)]
#[UsesClass(WorkerConfig::class)]
#[UsesClass(StopConditions::class)]
#[UsesClass(HandlerRegistry::class)]
#[UsesClass(CallableHandler::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(InMemoryEntry::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(OutgoingMessage::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(QueuedMessage::class)]
#[UsesClass(MaintenanceReport::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(UuidV7Generator::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(TextDecoder::class)]
#[UsesClass(DecodedText::class)]
#[UsesClass(ExponentialBackoff::class)]
#[UsesClass(MessageHandled::class)]
#[UsesClass(MessageReceived::class)]
#[UsesClass(WorkerStopped::class)]
final class QueueTest extends TestCase
{
    public function testProducerAndWorkerShareTheTransport(): void
    {
        $fakeClock = new FakeClock();
        $fakeSignalHandler = new FakeSignalHandler();
        $queue = $this->queue(new InMemoryTransport($fakeClock), $fakeClock, $fakeSignalHandler);

        $queue->producer()->publish('test', '{"n":1}');
        $handled = [];
        $exitReason = $queue->worker(new WorkerConfig(stopWhenEmpty: true))
            ->on('test', static function (Message $message, DecodedText $decodedText) use (&$handled): void {
                $handled[] = $decodedText->value;
            })
            ->run();

        self::assertSame(WorkerExitReason::QueueEmpty, $exitReason);
        self::assertSame([['n' => 1]], $handled);
        self::assertSame(1, $fakeSignalHandler->resets, 'the worker uses the queue signal handler');
        self::assertSame($queue->producer(), $queue->producer());
        self::assertNotSame($queue->worker(), $queue->worker());
    }

    public function testOptionalCapabilitiesAreExposedWhenTheTransportHasThem(): void
    {
        $fakeClock = new FakeClock();
        $inMemoryTransport = new InMemoryTransport($fakeClock);
        $queue = $this->queue($inMemoryTransport, $fakeClock);

        $queue->setup();
        $queue->ping();
        $queue->producer()->publishBatch([new OutgoingMessage('test', 'a')]);

        self::assertSame($inMemoryTransport, $queue->transport());
        self::assertSame($inMemoryTransport, $queue->deadLetters());
        self::assertSame(1, $queue->stats()?->ready);
        self::assertTrue($queue->capabilities()->deadLetter);

        self::assertSame(['a'], array_map(static fn (QueuedMessage $queuedMessage): string => $queuedMessage->message->text, $queue->browse() ?? []));
        self::assertSame(1, $queue->purge());
        self::assertSame([], $queue->browse(10));
    }

    public function testOptionalCapabilitiesAreNullWhenTheTransportLacksThem(): void
    {
        $recordingTransport = new RecordingTransport();
        $queue = $this->queue($recordingTransport, new FakeClock());

        $queue->setup();

        self::assertNull($queue->stats());
        self::assertNull($queue->deadLetters());
        self::assertNull($queue->purge());
        self::assertNull($queue->browse());
    }

    public function testSetupAndPingAreDelegated(): void
    {
        $transport = $this->createMockForIntersectionOfInterfaces([TransportInterface::class, SetupAwareInterface::class]);
        $transport->expects(self::once())->method('setup');
        $transport->expects(self::once())->method('ping');

        $queue = new Queue($transport, new QueueConfig(name: 'q'), new FakeClock(), new RecordingLogger(), new FakeSignalHandler(), new FakeMemoryMeter());
        $queue->setup();
        $queue->ping();
    }

    private function queue(InMemoryTransport|RecordingTransport $transport, FakeClock $fakeClock, ?FakeSignalHandler $fakeSignalHandler = null): Queue
    {
        return new Queue(
            $transport,
            new QueueConfig(name: 'q'),
            $fakeClock,
            new RecordingLogger(),
            $fakeSignalHandler ?? new FakeSignalHandler(),
            new FakeMemoryMeter(),
        );
    }
}
