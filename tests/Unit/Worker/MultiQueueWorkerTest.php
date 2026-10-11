<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Worker;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Retry\ExponentialBackoff;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\System\UuidV7Generator;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\FakeMemoryMeter;
use snipershady\Queue\Tests\Support\FakeSignalHandler;
use snipershady\Queue\Tests\Support\RecordingLogger;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Transport\InMemory\InMemoryBroker;
use snipershady\Queue\Transport\InMemory\InMemoryEntry;
use snipershady\Queue\Transport\InMemory\InMemoryTransport;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Worker\CallableHandler;
use snipershady\Queue\Worker\Event\MessageHandled;
use snipershady\Queue\Worker\Event\MessageReceived;
use snipershady\Queue\Worker\Event\MessageReleased;
use snipershady\Queue\Worker\HandlerRegistry;
use snipershady\Queue\Worker\MultiQueueWorker;
use snipershady\Queue\Worker\StopConditions;
use snipershady\Queue\Worker\Worker;
use snipershady\Queue\Worker\WorkerConfig;
use snipershady\Queue\Worker\WorkerExitReason;
use snipershady\Queue\Worker\WorkerInterface;

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
