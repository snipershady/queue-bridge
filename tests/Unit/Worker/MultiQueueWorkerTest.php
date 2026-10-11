<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Worker;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Retry\ExponentialBackoff;
use QueueBridge\System\NativeMemoryMeter;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\NullSignalHandler;
use QueueBridge\System\SystemClock;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\FakeMemoryMeter;
use QueueBridge\Tests\Support\FakeSignalHandler;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Text\DecodedText;
use QueueBridge\Text\TextDecoder;
use QueueBridge\Transport\InMemory\InMemoryBroker;
use QueueBridge\Transport\InMemory\InMemoryEntry;
use QueueBridge\Transport\InMemory\InMemoryTransport;
use QueueBridge\Transport\MaintenanceReport;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Worker\CallableHandler;
use QueueBridge\Worker\Event\MessageHandled;
use QueueBridge\Worker\Event\MessageReceived;
use QueueBridge\Worker\Event\MessageReleased;
use QueueBridge\Worker\HandlerRegistry;
use QueueBridge\Worker\MultiQueueWorker;
use QueueBridge\Worker\StopConditions;
use QueueBridge\Worker\Worker;
use QueueBridge\Worker\WorkerConfig;
use QueueBridge\Worker\WorkerExitReason;
use QueueBridge\Worker\WorkerInterface;

#[CoversClass(MultiQueueWorker::class)]
#[UsesClass(StopConditions::class)]
#[UsesClass(Worker::class)]
#[UsesClass(WorkerConfig::class)]
#[UsesClass(HandlerRegistry::class)]
#[UsesClass(CallableHandler::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(InMemoryEntry::class)]
#[UsesClass(MaintenanceReport::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(UuidV7Generator::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(TextDecoder::class)]
#[UsesClass(DecodedText::class)]
#[UsesClass(ExponentialBackoff::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(NullSignalHandler::class)]
#[UsesClass(NativeMemoryMeter::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(MessageHandled::class)]
#[UsesClass(MessageReceived::class)]
#[UsesClass(MessageReleased::class)]
#[UsesClass(ConnectionException::class)]
final class MultiQueueWorkerTest extends TestCase
{
    private FakeClock $fakeClock;

    private UuidV7Generator $uuidV7Generator;

    /** @var list<string> */
    private array $handled = [];

    private ?MultiQueueWorker $multi = null;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock();
        $this->uuidV7Generator = new UuidV7Generator($this->fakeClock);
    }

    public function testConsumesTheQueuesInTurnUntilTheyAreEmpty(): void
    {
        [$orders, $ordersWorker] = $this->queue('orders', batchSize: 1);
        [$emails, $emailsWorker] = $this->queue('emails', batchSize: 1);
        foreach (['o1', 'o2', 'o3'] as $text) {
            $this->send($orders, 'orders', $text);
        }

        $this->send($emails, 'emails', 'e1');

        $recordingLogger = new RecordingLogger();
        $multiQueueWorker = new MultiQueueWorker([$ordersWorker, $emailsWorker], new WorkerConfig(stopWhenEmpty: true), $this->fakeClock, new FakeSignalHandler(), new FakeMemoryMeter(), $recordingLogger);

        self::assertSame(WorkerExitReason::QueueEmpty, $multiQueueWorker->run());
        self::assertSame(['orders:o1', 'emails:e1', 'orders:o2', 'orders:o3'], $this->handled);
        self::assertSame(['reason' => 'queue_empty', 'processed' => 4], $recordingLogger->records[0]['context']);
    }

    public function testStopsAtTheGlobalMessageLimit(): void
    {
        [$orders, $ordersWorker] = $this->queue('orders', batchSize: 1);
        [$emails, $emailsWorker] = $this->queue('emails', batchSize: 1);
        foreach (['o1', 'o2', 'o3'] as $text) {
            $this->send($orders, 'orders', $text);
            $this->send($emails, 'emails', $text);
        }

        $multiQueueWorker = new MultiQueueWorker([$ordersWorker, $emailsWorker], new WorkerConfig(maxMessages: 3), $this->fakeClock);

        self::assertSame(WorkerExitReason::MessageLimitReached, $multiQueueWorker->run());
        self::assertCount(4, $this->handled, 'the limit is checked after each round');
    }

    public function testSignalStopsTheInnerWorkersAndReleasesTheirBatch(): void
    {
        $fakeSignalHandler = new FakeSignalHandler();
        [$orders, $ordersWorker] = $this->queue('orders', batchSize: 3, onHandle: static function () use ($fakeSignalHandler): void {
            $fakeSignalHandler->send();
        });
        [$emails, $emailsWorker] = $this->queue('emails', batchSize: 3);
        foreach (['o1', 'o2', 'o3'] as $text) {
            $this->send($orders, 'orders', $text);
        }

        $this->send($emails, 'emails', 'e1');

        $multiQueueWorker = new MultiQueueWorker([$ordersWorker, $emailsWorker], new WorkerConfig(), $this->fakeClock, $fakeSignalHandler, new FakeMemoryMeter(), new RecordingLogger());

        self::assertSame(WorkerExitReason::StopRequested, $multiQueueWorker->run());
        self::assertSame(['orders:o1'], $this->handled, 'the second queue is not visited after the stop');
        self::assertSame(2, $orders->stats()->ready, 'the rest of the batch was released');
        self::assertSame(1, $emails->stats()->ready);
        self::assertSame(1, $fakeSignalHandler->resets);
    }

    public function testSignalIsLogged(): void
    {
        $fakeSignalHandler = new FakeSignalHandler();
        $worker = self::createStub(WorkerInterface::class);
        $worker->method('runOnce')->willReturnCallback(static function () use ($fakeSignalHandler): int {
            $fakeSignalHandler->send(\SIGINT);

            return 1;
        });
        $recordingLogger = new RecordingLogger();

        (new MultiQueueWorker([$worker], new WorkerConfig(), $this->fakeClock, $fakeSignalHandler, new FakeMemoryMeter(), $recordingLogger))->run();

        self::assertSame('notice', $recordingLogger->records[0]['level']);
        self::assertSame(['signal' => \SIGINT], $recordingLogger->records[0]['context']);
    }

    public function testSignalHandlerIsResetWhenABackendErrorEscapes(): void
    {
        $fakeSignalHandler = new FakeSignalHandler();
        $worker = self::createStub(WorkerInterface::class);
        $worker->method('runOnce')->willThrowException(ConnectionException::unreachable('Test', 'down'));

        try {
            (new MultiQueueWorker([$worker], new WorkerConfig(), $this->fakeClock, $fakeSignalHandler))->run();
            self::fail('Expected ConnectionException');
        } catch (ConnectionException) {
            self::assertSame(1, $fakeSignalHandler->resets);
        }
    }

    public function testStopIsForwardedToCustomWorkerImplementations(): void
    {
        $worker = $this->createMock(WorkerInterface::class);
        $worker->expects(self::once())->method('stop');
        $worker->expects(self::once())->method('runOnce')->willReturnCallback(function (): int {
            $this->multi?->stop();

            return 1;
        });
        $this->multi = new MultiQueueWorker([$worker], new WorkerConfig(), $this->fakeClock);

        self::assertSame(WorkerExitReason::StopRequested, $this->multi->run());
    }

    public function testRunOnceReturnsTheMessagesOfOneRound(): void
    {
        [$orders, $ordersWorker] = $this->queue('orders', batchSize: 10);
        $this->send($orders, 'orders', 'o1');
        $this->send($orders, 'orders', 'o2');

        self::assertSame(2, (new MultiQueueWorker([$ordersWorker]))->runOnce());
    }

    public function testRequiresAtLeastOneWorker(): void
    {
        $this->expectException(InvalidArgumentException::class);

        /* @phpstan-ignore argument.type (verifica a runtime di un input che viola il tipo dichiarato) */
        new MultiQueueWorker([]);
    }

    /**
     * @param ?callable(): void $onHandle
     *
     * @return array{InMemoryTransport, Worker}
     */
    private function queue(string $name, int $batchSize, ?callable $onHandle = null): array
    {
        $queueConfig = new QueueConfig(name: $name);
        $inMemoryTransport = new InMemoryTransport($this->fakeClock, $queueConfig);
        $worker = (new Worker($inMemoryTransport, $queueConfig, new WorkerConfig(batchSize: $batchSize, receiveTimeoutMs: 0), $this->fakeClock, new FakeSignalHandler(), new FakeMemoryMeter()))
            ->on($name, function (Message $message) use ($onHandle): void {
                $this->handled[] = $message->channel.':'.$message->text;
                if (null !== $onHandle) {
                    $onHandle();
                }
            });

        return [$inMemoryTransport, $worker];
    }

    private function send(InMemoryTransport $inMemoryTransport, string $channel, string $text): void
    {
        $inMemoryTransport->send(new Message($this->uuidV7Generator->generate(), $channel, $text, $this->fakeClock->now()));
    }
}
