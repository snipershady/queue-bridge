<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Worker;

use DateTimeImmutable;
use InvalidArgumentException;
use LogicException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Exception\SerializationException;
use snipershady\Queue\Exception\UnknownChannelException;
use snipershady\Queue\Exception\UnrecoverableMessageException;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Middleware\MiddlewareInterface;
use snipershady\Queue\Middleware\TimeoutMiddleware;
use snipershady\Queue\Retry\ExponentialBackoff;
use snipershady\Queue\Retry\FixedDelay;
use snipershady\Queue\Retry\NoRetry;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\System\UuidV7Generator;
use snipershady\Queue\Tests\Support\CallLog;
use snipershady\Queue\Tests\Support\FakeAlarm;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\FakeMemoryMeter;
use snipershady\Queue\Tests\Support\FakeSignalHandler;
use snipershady\Queue\Tests\Support\RecordingListener;
use snipershady\Queue\Tests\Support\RecordingLogger;
use snipershady\Queue\Tests\Support\RecordingTransport;
use snipershady\Queue\Tests\Support\SpyTransport;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Transport\DeadLetter;
use snipershady\Queue\Transport\InMemory\InMemoryBroker;
use snipershady\Queue\Transport\InMemory\InMemoryEntry;
use snipershady\Queue\Transport\InMemory\InMemoryTransport;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;
use snipershady\Queue\Worker\CallableHandler;
use snipershady\Queue\Worker\Event\MessageDeadLettered;
use snipershady\Queue\Worker\Event\MessageDiscarded;
use snipershady\Queue\Worker\Event\MessageFailed;
use snipershady\Queue\Worker\Event\MessageHandled;
use snipershady\Queue\Worker\Event\MessageReceived;
use snipershady\Queue\Worker\Event\MessageReleased;
use snipershady\Queue\Worker\Event\MessageRetried;
use snipershady\Queue\Worker\Event\WorkerEventInterface;
use snipershady\Queue\Worker\Event\WorkerEventListenerInterface;
use snipershady\Queue\Worker\Event\WorkerStopped;
use snipershady\Queue\Worker\ExpiredMessagePolicy;
use snipershady\Queue\Worker\HandlerRegistry;
use snipershady\Queue\Worker\StopConditions;
use snipershady\Queue\Worker\UnknownChannelPolicy;
use snipershady\Queue\Worker\Worker;
use snipershady\Queue\Worker\WorkerConfig;
use snipershady\Queue\Worker\WorkerExitReason;

#[CoversClass(Worker::class)]
#[CoversClass(StopConditions::class)]
#[CoversClass(MessageReceived::class)]
#[CoversClass(MessageHandled::class)]
#[CoversClass(MessageFailed::class)]
#[CoversClass(MessageRetried::class)]
#[CoversClass(MessageDeadLettered::class)]
#[CoversClass(MessageDiscarded::class)]
#[CoversClass(MessageReleased::class)]
#[CoversClass(WorkerStopped::class)]
#[UsesClass(HandlerRegistry::class)]
#[UsesClass(CallableHandler::class)]
#[UsesClass(WorkerConfig::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(FailureReason::class)]
#[UsesClass(DeadLetter::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(InMemoryEntry::class)]
#[UsesClass(MaintenanceReport::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(UuidV7Generator::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(TextDecoder::class)]
#[UsesClass(DecodedText::class)]
#[UsesClass(ExponentialBackoff::class)]
#[UsesClass(FixedDelay::class)]
#[UsesClass(NoRetry::class)]
#[UsesClass(RetryableException::class)]
#[UsesClass(UnrecoverableMessageException::class)]
#[UsesClass(UnknownChannelException::class)]
#[UsesClass(SerializationException::class)]
#[UsesClass(TimeoutMiddleware::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(NullSignalHandler::class)]
#[UsesClass(NativeMemoryMeter::class)]
final class WorkerTest extends TestCase
{
    private FakeClock $fakeClock;

    private QueueConfig $queueConfig;

    private InMemoryTransport $inMemoryTransport;

    private SpyTransport $spyTransport;

    private RecordingListener $recordingListener;

    private RecordingLogger $recordingLogger;

    private UuidV7Generator $uuidV7Generator;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock();
        $this->queueConfig = new QueueConfig(maxAttempts: 3, maxDeliveries: 5, claimIdleMs: 10_000);
        $this->inMemoryTransport = new InMemoryTransport($this->fakeClock, $this->queueConfig);
        $this->spyTransport = new SpyTransport($this->inMemoryTransport);
        $this->recordingListener = new RecordingListener();
        $this->recordingLogger = new RecordingLogger();
        $this->uuidV7Generator = new UuidV7Generator($this->fakeClock);
    }

    // --- successo -------------------------------------------------------------------------------------------

    public function testHandlesAMessageAndAcknowledgesIt(): void
    {
        $received = [];
        $worker = $this->worker()->on('test', static function (Message $message, DecodedText $decodedText) use (&$received): void {
            $received[] = [$message->text, $decodedText->isJson(), $decodedText->value];
        });
        $this->send('test', '{"a":1}');

        self::assertSame(1, $worker->runOnce());

        self::assertSame([['{"a":1}', true, ['a' => 1]]], $received);
        self::assertSame(0, $this->inMemoryTransport->stats()->inFlight);
        self::assertSame(['MessageReceived', 'MessageHandled'], $this->recordingListener->names());
    }

    public function testHandledEventCarriesTheDuration(): void
    {
        $worker = $this->worker()->on('test', function (): void {
            $this->fakeClock->advanceMs(25);
        });
        $this->send('test', 'x');

        $worker->runOnce();

        [$messageHandled] = $this->recordingListener->ofType(MessageHandled::class);
        self::assertSame(25, $messageHandled->durationMs);
        self::assertSame('x', $messageHandled->delivery->message->text);
        self::assertSame('x', $this->recordingListener->ofType(MessageReceived::class)[0]->delivery->message->text);
    }

    public function testFallbackHandlerReceivesUnregisteredChannels(): void
    {
        $channels = [];
        $worker = $this->worker()
            ->on('orders', static function (): void {
            })
            ->on('*', static function (Message $message) use (&$channels): void {
                $channels[] = $message->channel;
            });
        $this->send('orders', 'a');
        $this->send('invoices', 'b');

        self::assertSame(2, $worker->runOnce());
        self::assertSame(['invoices'], $channels);
    }

    public function testRunOnceWithEmptyQueueReturnsZero(): void
    {
        self::assertSame(0, $this->worker()->runOnce());
    }

    public function testAcknowledgesInBatchWhenTheTransportSupportsIt(): void
    {
        $worker = $this->worker()->on('test', static function (): void {
        });
        $this->send('test', 'a');
        $this->send('test', 'b');

        $worker->runOnce();

        self::assertSame([2], $this->spyTransport->ackBatches);
        self::assertSame(0, $this->spyTransport->acks);
    }

    public function testAcknowledgesOneByOneWhenBatchAckIsNotAvailable(): void
    {
        $spyTransport = new SpyTransport($this->inMemoryTransport, batchAck: false);
        $worker = $this->worker(transport: $spyTransport)->on('test', static function (): void {
        });
        $this->send('test', 'a');
        $this->send('test', 'b');

        $worker->runOnce();

        self::assertSame(2, $spyTransport->acks);
        self::assertSame([], $spyTransport->ackBatches);
    }

    public function testMiddlewaresWrapTheHandlerInOrder(): void
    {
        $callLog = new CallLog();
        $worker = $this->worker(new WorkerConfig(
            middlewares: [$this->middleware('outer', $callLog), $this->middleware('inner', $callLog)],
            listeners: [$this->recordingListener],
        ))->on('test', static function () use ($callLog): void {
            $callLog->record('handler');
        });
        $this->send('test', 'x');

        $worker->runOnce();

        self::assertSame(['outer:before', 'inner:before', 'handler', 'inner:after', 'outer:after'], $callLog->calls);
    }

    public function testMiddlewareThatSkipsTheHandlerStillAcknowledges(): void
    {
        $skip = new class implements MiddlewareInterface {
            #[Override]
            public function process(Message $message, callable $next): void
            {
            }
        };
        $worker = $this->worker(new WorkerConfig(middlewares: [$skip], listeners: [$this->recordingListener]))
            ->on('test', static function (): never {
                throw new LogicException('must not run');
            });
        $this->send('test', 'x');

        $worker->runOnce();

        self::assertSame(['MessageReceived', 'MessageHandled'], $this->recordingListener->names());
        self::assertSame(0, $this->inMemoryTransport->stats()->inFlight);
    }

    // --- fallimenti ------------------------------------------------------------------------------------------

    public function testRetryableExceptionIsRetriedWithTheStrategyDelay(): void
    {
        $worker = $this->worker(new WorkerConfig(retryStrategy: new FixedDelay(1500), listeners: [$this->recordingListener]))
            ->on('test', static function (): never {
                throw new RetryableException('db down');
            });
        $this->send('test', 'x');

        $worker->runOnce();

        self::assertSame(['MessageReceived', 'MessageFailed', 'MessageRetried'], $this->recordingListener->names());
        self::assertSame(1500, $this->recordingListener->ofType(MessageRetried::class)[0]->delayMs);
        self::assertSame('db down', $this->recordingListener->ofType(MessageFailed::class)[0]->error->getMessage());
        self::assertSame(1, $this->inMemoryTransport->stats()->delayed);
        self::assertContains('warning', $this->recordingLogger->levels());
    }

    public function testRetryableExceptionDelayOverridesTheStrategy(): void
    {
        $worker = $this->worker(new WorkerConfig(retryStrategy: new NoRetry(), listeners: [$this->recordingListener]))
            ->on('test', static function (): never {
                throw new RetryableException('later', 0);
            });
        $this->send('test', 'x');

        $worker->runOnce();
        self::assertSame(0, $this->recordingListener->ofType(MessageRetried::class)[0]->delayMs);

        $worker->runOnce();
        $worker->runOnce();

        // Terzo tentativo = maxAttempts: in DLQ con attempts = 2.
        [$deadLetter] = $this->inMemoryTransport->deadLetters();
        self::assertSame(2, $deadLetter->message->attempts);
        self::assertSame(FailureKind::HandlerFailed, $deadLetter->reason->kind);
        self::assertSame(RetryableException::class, $deadLetter->reason->errorClass);
    }

    public function testRetriesStopAtMaxAttempts(): void
    {
        $runs = 0;
        $worker = $this->worker(new WorkerConfig(retryStrategy: new FixedDelay(0), listeners: [$this->recordingListener]))
            ->on('test', static function () use (&$runs): never {
                ++$runs;

                throw new RetryableException('again');
            });
        $this->send('test', 'x');

        for ($i = 0; $i < 5; ++$i) {
            $worker->runOnce();
        }

        self::assertSame(3, $runs);
        self::assertCount(2, $this->recordingListener->ofType(MessageRetried::class));
        self::assertCount(1, $this->recordingListener->ofType(MessageDeadLettered::class));
        self::assertSame(['error'], \array_slice($this->recordingLogger->levels(), -1));
    }

    public function testStrategyReturningNullSendsToTheDeadLetterQueue(): void
    {
        $worker = $this->worker(new WorkerConfig(retryStrategy: new NoRetry()))
            ->on('test', static function (): never {
                throw new RetryableException('no luck');
            });
        $this->send('test', 'x');

        $worker->runOnce();

        self::assertCount(1, $this->inMemoryTransport->deadLetters());
        self::assertSame(0, $this->inMemoryTransport->stats()->delayed);
    }

    public function testGenericExceptionGoesToTheDeadLetterQueueByDefault(): void
    {
        $worker = $this->worker()->on('test', static function (): never {
            throw new RuntimeException('bug');
        });
        $this->send('test', 'x');

        $worker->runOnce();

        [$deadLetter] = $this->inMemoryTransport->deadLetters();
        self::assertSame(FailureKind::HandlerFailed, $deadLetter->reason->kind);
        self::assertSame('bug', $deadLetter->reason->errorMessage);
        self::assertSame(RuntimeException::class, $deadLetter->reason->errorClass);
    }

    public function testGenericExceptionIsRetriedWithRetryAnyThrowable(): void
    {
        $worker = $this->worker(new WorkerConfig(retryStrategy: new FixedDelay(0), retryAnyThrowable: true))
            ->on('test', static function (): never {
                throw new RuntimeException('transient');
            });
        $this->send('test', 'x');

        $worker->runOnce();

        self::assertSame([], $this->inMemoryTransport->deadLetters());
        [$delivery] = $this->inMemoryTransport->receive();
        self::assertSame(1, $delivery->message->attempts);
    }

    public function testUnrecoverableExceptionIsNeverRetried(): void
    {
        $worker = $this->worker(new WorkerConfig(retryAnyThrowable: true))->on('test', static function (): never {
            throw new UnrecoverableMessageException('invalid order');
        });
        $this->send('test', 'x');

        $worker->runOnce();

        [$deadLetter] = $this->inMemoryTransport->deadLetters();
        self::assertSame(FailureKind::Unrecoverable, $deadLetter->reason->kind);
        self::assertSame(0, $deadLetter->message->attempts);
    }

    public function testSerializationExceptionIsNeverRetried(): void
    {
        $worker = $this->worker(new WorkerConfig(retryAnyThrowable: true))->on('test', static function (): never {
            throw SerializationException::invalidField('payload', 'bad.');
        });
        $this->send('test', 'x');

        $worker->runOnce();

        self::assertSame(FailureKind::Serialization, $this->inMemoryTransport->deadLetters()[0]->reason->kind);
    }

    public function testBackendErrorWhileAcknowledgingIsNotTreatedAsAHandlerFailure(): void
    {
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming[] = [$this->delivery('test', 'x')];
        $recordingTransport->failOnAck = new RuntimeException('connection lost');
        $worker = $this->worker(transport: $recordingTransport)->on('test', static function (): void {
        });

        try {
            $worker->runOnce();
            self::fail('Expected RuntimeException');
        } catch (RuntimeException $runtimeException) {
            self::assertSame('connection lost', $runtimeException->getMessage());
        }

        self::assertSame([], $recordingTransport->retried);
        self::assertSame([], $recordingTransport->rejected);
    }

    // --- controlli prima dell'handler ------------------------------------------------------------------------

    public function testMessageDeliveredTooManyTimesGoesToTheDeadLetterQueue(): void
    {
        $runs = 0;
        $worker = $this->worker()->on('test', static function () use (&$runs): void {
            ++$runs;
        });
        $this->send('test', 'poison');

        // Il worker "crasha" 5 volte (= maxDeliveries): ricezione senza ack, poi recupero dell'orfano.
        for ($i = 0; $i < 5; ++$i) {
            $this->inMemoryTransport->receive();
            $this->fakeClock->advanceMs(10_000);
            $this->inMemoryTransport->maintain();
        }

        $worker->runOnce();

        self::assertSame(0, $runs);
        [$deadLetter] = $this->inMemoryTransport->deadLetters();
        self::assertSame(FailureKind::MaxDeliveriesExceeded, $deadLetter->reason->kind);
        self::assertSame('Delivered 6 times without acknowledgement (max 5).', $deadLetter->reason->errorMessage);
        self::assertNull($deadLetter->reason->errorClass);
    }

    public function testMessageDeliveredExactlyMaxDeliveriesTimesIsStillHandled(): void
    {
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming[] = [$this->delivery('test', 'x', deliveryCount: 5)];
        $runs = 0;
        $worker = $this->worker(transport: $recordingTransport)->on('test', static function () use (&$runs): void {
            ++$runs;
        });

        $worker->runOnce();

        self::assertSame(1, $runs);
        self::assertSame([], $recordingTransport->rejected);
    }

    public function testRejectedMessagesAreNeverAlsoAcknowledged(): void
    {
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming[] = [$this->delivery('unrecoverable', 'a'), $this->delivery('serialization', 'b'), $this->delivery('bug', 'c')];
        $worker = $this->worker(transport: $recordingTransport)
            ->on('unrecoverable', static function (): never {
                throw new UnrecoverableMessageException('no');
            })
            ->on('serialization', static function (): never {
                throw SerializationException::unsupportedVersion('9');
            })
            ->on('bug', static function (): never {
                throw new RuntimeException('bug');
            });

        self::assertSame(3, $worker->runOnce());

        self::assertSame([], $recordingTransport->acked);
        self::assertSame(
            [FailureKind::Unrecoverable, FailureKind::Serialization, FailureKind::HandlerFailed],
            array_map(static fn (array $rejected): FailureKind => $rejected[1]->kind, $recordingTransport->rejected),
        );
    }

    public function testDeliveryCountIsIgnoredWhenTheTransportCannotProvideIt(): void
    {
        $recordingTransport = new RecordingTransport(new TransportCapabilities(OrphanRecovery::Native, deliveryCount: false));
        $recordingTransport->incoming[] = [$this->delivery('test', 'x', deliveryCount: 99)];
        $runs = 0;
        $worker = $this->worker(transport: $recordingTransport)->on('test', static function () use (&$runs): void {
            ++$runs;
        });

        $worker->runOnce();

        self::assertSame(1, $runs);
        self::assertCount(1, $recordingTransport->acked);
    }

    public function testExpiredMessageIsDiscardedByDefault(): void
    {
        $runs = 0;
        $worker = $this->worker()->on('test', static function () use (&$runs): void {
            ++$runs;
        });
        $this->send('test', 'stale', ttlMs: 1000);
        $this->fakeClock->advanceMs(1000);

        $worker->runOnce();

        self::assertSame(0, $runs);
        self::assertSame([], $this->inMemoryTransport->deadLetters());
        self::assertSame(0, $this->inMemoryTransport->stats()->inFlight);
        self::assertSame(FailureKind::Expired, $this->recordingListener->ofType(MessageDiscarded::class)[0]->reason);
        self::assertSame('stale', $this->recordingListener->ofType(MessageDiscarded::class)[0]->delivery->message->text);
    }

    public function testExpiredMessageCanGoToTheDeadLetterQueue(): void
    {
        $worker = $this->worker(new WorkerConfig(expiredMessagePolicy: ExpiredMessagePolicy::DeadLetter))->on('test', static function (): void {
        });
        $this->send('test', 'stale', ttlMs: 1000);
        $this->fakeClock->advanceMs(5000);

        $worker->runOnce();

        [$deadLetter] = $this->inMemoryTransport->deadLetters();
        self::assertSame(FailureKind::Expired, $deadLetter->reason->kind);
        self::assertSame('Message expired before processing.', $deadLetter->reason->errorMessage);
    }

    public function testUnknownChannelGoesToTheDeadLetterQueueByDefault(): void
    {
        $worker = $this->worker()->on('known', static function (): void {
        });
        $this->send('unknown', 'x');

        $worker->runOnce();

        [$deadLetter] = $this->inMemoryTransport->deadLetters();
        self::assertSame(FailureKind::UnknownChannel, $deadLetter->reason->kind);
        self::assertSame(UnknownChannelException::class, $deadLetter->reason->errorClass);
        self::assertSame('No handler registered for channel "unknown".', $deadLetter->reason->errorMessage);
        self::assertSame($deadLetter->reason, $this->recordingListener->ofType(MessageDeadLettered::class)[0]->reason);
    }

    public function testUnknownChannelCanBeDiscarded(): void
    {
        $worker = $this->worker(new WorkerConfig(unknownChannelPolicy: UnknownChannelPolicy::Discard, listeners: [$this->recordingListener]));
        $this->send('unknown', 'x');

        $worker->runOnce();

        self::assertSame([], $this->inMemoryTransport->deadLetters());
        self::assertSame(FailureKind::UnknownChannel, $this->recordingListener->ofType(MessageDiscarded::class)[0]->reason);
    }

    // --- manutenzione, gc, listener --------------------------------------------------------------------------

    public function testMaintenanceRunsOnTheFirstCycleAndThenAtTheConfiguredInterval(): void
    {
        $worker = $this->worker(new WorkerConfig(maintenanceIntervalMs: 1000));

        $worker->runOnce();

        $this->fakeClock->advanceMs(999);
        $worker->runOnce();
        self::assertSame(1, $this->spyTransport->maintenances);

        $this->fakeClock->advanceMs(1);
        $worker->runOnce();
        self::assertSame(2, $this->spyTransport->maintenances);
    }

    public function testTransportsWithoutMaintenanceAreNotMaintained(): void
    {
        $recordingTransport = new RecordingTransport();

        self::assertSame(0, $this->worker(transport: $recordingTransport)->runOnce());
        self::assertSame([[10, 1000]], $recordingTransport->receiveCalls);
    }

    public function testGarbageIsCollectedEveryNMessages(): void
    {
        $fakeMemoryMeter = new FakeMemoryMeter();
        $worker = $this->worker(new WorkerConfig(gcEveryMessages: 3), memoryMeter: $fakeMemoryMeter)->on('test', static function (): void {
        });

        foreach (['a', 'b'] as $text) {
            $this->send('test', $text);
        }

        $worker->runOnce();
        self::assertSame(0, $fakeMemoryMeter->collections);

        $this->send('test', 'c');
        $worker->runOnce();
        self::assertSame(1, $fakeMemoryMeter->collections);
    }

    public function testGarbageCollectionCounterRestartsAfterEachCollection(): void
    {
        $fakeMemoryMeter = new FakeMemoryMeter();
        $worker = $this->worker(new WorkerConfig(batchSize: 1, gcEveryMessages: 2), memoryMeter: $fakeMemoryMeter)->on('test', static function (): void {
        });
        foreach (['a', 'b', 'c', 'd'] as $text) {
            $this->send('test', $text);
        }

        for ($i = 0; $i < 4; ++$i) {
            $worker->runOnce();
        }

        self::assertSame(2, $fakeMemoryMeter->collections);
    }

    public function testGarbageCollectionCanBeDisabled(): void
    {
        $fakeMemoryMeter = new FakeMemoryMeter();
        $worker = $this->worker(new WorkerConfig(gcEveryMessages: 0), memoryMeter: $fakeMemoryMeter)->on('test', static function (): void {
        });
        $this->send('test', 'a');

        $worker->runOnce();

        self::assertSame(0, $fakeMemoryMeter->collections);
    }

    public function testFailingListenerIsLoggedAndDoesNotStopTheWorker(): void
    {
        $failing = new class implements WorkerEventListenerInterface {
            #[Override]
            public function onEvent(WorkerEventInterface $event): void
            {
                throw new RuntimeException('listener bug');
            }
        };
        $worker = $this->worker(new WorkerConfig(listeners: [$failing, $this->recordingListener]))->on('test', static function (): void {
        });
        $this->send('test', 'x');

        self::assertSame(1, $worker->runOnce());
        self::assertSame(['MessageReceived', 'MessageHandled'], $this->recordingListener->names());
        self::assertSame(['error', 'error'], $this->recordingLogger->levels());
        self::assertSame('listener bug', $this->recordingLogger->records[0]['context']['error']);
    }

    // --- run() e condizioni di uscita ------------------------------------------------------------------------

    public function testRunStopsAtMaxMessagesWithoutOverReceiving(): void
    {
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming = [
            [$this->delivery('test', 'a'), $this->delivery('test', 'b')],
            [$this->delivery('test', 'c')],
        ];
        $worker = $this->worker(new WorkerConfig(batchSize: 2, maxMessages: 3, listeners: [$this->recordingListener]), transport: $recordingTransport)
            ->on('test', static function (): void {
            });

        self::assertSame(WorkerExitReason::MessageLimitReached, $worker->run());
        self::assertSame([[2, 1000], [1, 1000]], $recordingTransport->receiveCalls);
        $workerStopped = $this->recordingListener->ofType(WorkerStopped::class)[0];
        self::assertSame(WorkerExitReason::MessageLimitReached, $workerStopped->reason);
        self::assertSame(3, $workerStopped->processed);
        self::assertSame(['info'], \array_slice($this->recordingLogger->levels(), -1));
    }

    public function testRunStopsAtMaxRuntime(): void
    {
        foreach (['a', 'b', 'c'] as $text) {
            $this->send('test', $text);
        }

        $worker = $this->worker(new WorkerConfig(batchSize: 1, maxRuntimeSeconds: 2))->on('test', function (): void {
            $this->fakeClock->advanceMs(1000);
        });

        self::assertSame(WorkerExitReason::TimeLimitReached, $worker->run());
        self::assertSame(1, $this->inMemoryTransport->stats()->ready);
    }

    public function testRunStopsAboveTheMemoryLimit(): void
    {
        $fakeMemoryMeter = new FakeMemoryMeter(64 * 1024 * 1024);
        $worker = $this->worker(new WorkerConfig(memoryLimitMb: 64), memoryMeter: $fakeMemoryMeter);

        self::assertSame(WorkerExitReason::MemoryLimitReached, $worker->run());

        $fakeMemoryMeter->usage = 64 * 1024 * 1024 - 1;
        $worker = $this->worker(new WorkerConfig(memoryLimitMb: 64, stopWhenEmpty: true), memoryMeter: $fakeMemoryMeter);
        self::assertSame(WorkerExitReason::QueueEmpty, $worker->run());
    }

    public function testRunStopsWhenEmpty(): void
    {
        $handled = [];
        $worker = $this->worker(new WorkerConfig(batchSize: 2, stopWhenEmpty: true))->on('test', static function (Message $message) use (&$handled): void {
            $handled[] = $message->text;
        });
        foreach (['a', 'b', 'c'] as $text) {
            $this->send('test', $text);
        }

        self::assertSame(WorkerExitReason::QueueEmpty, $worker->run());
        self::assertSame(['a', 'b', 'c'], $handled);
    }

    public function testSignalStopsTheWorkerAndReleasesTheRestOfTheBatch(): void
    {
        $fakeSignalHandler = new FakeSignalHandler();
        $handled = [];
        $worker = $this->worker(new WorkerConfig(batchSize: 3, listeners: [$this->recordingListener]), signalHandler: $fakeSignalHandler)
            ->on('test', static function (Message $message) use ($fakeSignalHandler, &$handled): void {
                $handled[] = $message->text;
                $fakeSignalHandler->send();
            });
        foreach (['a', 'b', 'c'] as $text) {
            $this->send('test', $text);
        }

        self::assertSame(WorkerExitReason::StopRequested, $worker->run());

        self::assertSame(['a'], $handled);
        self::assertSame(1, $fakeSignalHandler->resets);
        self::assertSame(['b', 'c'], array_map(static fn (MessageReleased $messageReleased): string => $messageReleased->delivery->message->text, $this->recordingListener->ofType(MessageReleased::class)));
        self::assertSame(2, $this->inMemoryTransport->stats()->ready);
        self::assertSame(0, $this->inMemoryTransport->stats()->inFlight);
        // L'ack di "a" è stato inviato prima di rilasciare gli altri.
        self::assertSame([1], $this->spyTransport->ackBatches);
        self::assertSame('notice', $this->recordingLogger->levels()[0]);
        self::assertSame(\SIGTERM, $this->recordingLogger->records[0]['context']['signal']);
    }

    public function testRunOnceCountsOnlyProcessedMessages(): void
    {
        $worker = null;
        $worker = $this->worker(new WorkerConfig(batchSize: 3))->on('test', static function () use (&$worker): void {
            self::assertInstanceOf(Worker::class, $worker);
            $worker->stop();
        });
        foreach (['a', 'b', 'c'] as $text) {
            $this->send('test', $text);
        }

        self::assertSame(1, $worker->runOnce());
        self::assertSame(2, $this->inMemoryTransport->stats()->ready);
    }

    public function testDecisionsAreLoggedWithContext(): void
    {
        $worker = $this->worker(new WorkerConfig(stopWhenEmpty: true, retryStrategy: new FixedDelay(100), unknownChannelPolicy: UnknownChannelPolicy::Discard))
            ->on('retry', static function (): never {
                throw new RetryableException('flaky');
            })
            ->on('dlq', static function (): never {
                throw new UnrecoverableMessageException('broken');
            });
        $this->send('retry', 'a');
        $this->send('dlq', 'b');
        $this->send('nobody', 'c');

        $worker->run();

        $contexts = array_column($this->recordingLogger->records, 'context');
        self::assertSame(['attempt' => 1, 'max_attempts' => 3, 'delay_ms' => 100, 'error' => 'flaky'], array_diff_key($contexts[0], ['message_id' => true]));
        self::assertSame(['kind' => 'unrecoverable', 'error' => 'broken'], array_diff_key($contexts[1], ['message_id' => true]));
        self::assertSame(['reason' => 'unknown_channel'], array_diff_key($contexts[2], ['message_id' => true]));
        self::assertSame(['reason' => 'queue_empty', 'processed' => 3], $contexts[3]);
        foreach ([0, 1, 2] as $index) {
            self::assertArrayHasKey('message_id', $contexts[$index]);
        }
    }

    public function testStopBeforeRunIsForgottenWhenRunStarts(): void
    {
        $worker = $this->worker(new WorkerConfig(stopWhenEmpty: true));
        $worker->stop();

        self::assertSame(WorkerExitReason::QueueEmpty, $worker->run());
    }

    public function testStopFromAListenerEndsRun(): void
    {
        $worker = null;
        $stopper = new class($worker) implements WorkerEventListenerInterface {
            public function __construct(public ?Worker &$worker)
            {
            }

            #[Override]
            public function onEvent(WorkerEventInterface $event): void
            {
                if ($event instanceof MessageHandled) {
                    $this->worker?->stop();
                }
            }
        };
        $worker = $this->worker(new WorkerConfig(listeners: [$stopper]))->on('test', static function (): void {
        });
        $this->send('test', 'a');

        self::assertSame(WorkerExitReason::StopRequested, $worker->run());
    }

    public function testBackendErrorEscapesRunAfterResettingSignals(): void
    {
        $fakeSignalHandler = new FakeSignalHandler();
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming[] = [$this->delivery('test', 'x')];
        $recordingTransport->failOnAck = new RuntimeException('down');
        $worker = $this->worker(transport: $recordingTransport, signalHandler: $fakeSignalHandler)->on('test', static function (): void {
        });

        try {
            $worker->run();
            self::fail('Expected RuntimeException');
        } catch (RuntimeException) {
            self::assertSame(1, $fakeSignalHandler->resets);
        }
    }

    // --- batch e claimIdleMs ---------------------------------------------------------------------------------

    public function testMessagesWaitingTooLongInTheBatchAreReleasedBeforeClaimIdle(): void
    {
        // claimIdleMs = 10 s e nessun timeout dell'handler: si inizia un messaggio solo entro 5 s dalla ricezione.
        $handled = [];
        $worker = $this->worker(new WorkerConfig(batchSize: 3, listeners: [$this->recordingListener]))
            ->on('test', function (Message $message) use (&$handled): void {
                $handled[] = $message->text;
                $this->fakeClock->advanceMs(5000);
            });
        foreach (['a', 'b', 'c'] as $text) {
            $this->send('test', $text);
        }

        self::assertSame(1, $worker->runOnce());

        self::assertSame(['a'], $handled);
        self::assertSame(['b', 'c'], array_map(static fn (MessageReleased $messageReleased): string => $messageReleased->delivery->message->text, $this->recordingListener->ofType(MessageReleased::class)));
        self::assertSame(2, $this->inMemoryTransport->stats()->ready);
        self::assertSame(0, $this->inMemoryTransport->stats()->inFlight);
        // L'ack di "a" parte prima dei rilasci, non a fine batch.
        self::assertSame([1], $this->spyTransport->ackBatches);
        self::assertSame(['warning'], $this->recordingLogger->levels());
        self::assertSame(['released' => 2, 'deadline_ms' => 5000], $this->recordingLogger->records[0]['context']);
    }

    public function testMessageStartingJustBeforeTheDeadlineIsHandled(): void
    {
        $handled = [];
        $worker = $this->worker(new WorkerConfig(batchSize: 2))->on('test', function (Message $message) use (&$handled): void {
            $handled[] = $message->text;
            $this->fakeClock->advanceMs(4999);
        });
        $this->send('test', 'a');
        $this->send('test', 'b');

        self::assertSame(2, $worker->runOnce());

        self::assertSame(['a', 'b'], $handled);
        self::assertSame([], $this->recordingLogger->records);
    }

    public function testHandlerTimeoutSetsTheDeadlineBeforeClaimIdle(): void
    {
        // Vale il timeout più lungo (2 s), ovunque sia nella pipeline: un messaggio iniziato entro 8 s finisce
        // comunque prima dei 10 s di claimIdleMs.
        $middlewares = [$this->middleware('log', new CallLog()), new TimeoutMiddleware(new FakeAlarm(), 2), new TimeoutMiddleware(new FakeAlarm(), 1)];
        $handled = [];
        $worker = $this->worker(new WorkerConfig(batchSize: 3, middlewares: $middlewares))
            ->on('test', function (Message $message) use (&$handled): void {
                $handled[] = $message->text;
                $this->fakeClock->advanceMs(4000);
            });
        foreach (['a', 'b', 'c'] as $text) {
            $this->send('test', $text);
        }

        self::assertSame(2, $worker->runOnce());

        self::assertSame(['a', 'b'], $handled);
        self::assertSame(['released' => 1, 'deadline_ms' => 8000], $this->recordingLogger->records[0]['context']);
    }

    public function testBackendsWithNativeOrphanRecoveryHaveNoBatchDeadline(): void
    {
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming[] = [$this->delivery('test', 'a'), $this->delivery('test', 'b')];
        $worker = $this->worker(transport: $recordingTransport)->on('test', function (): void {
            $this->fakeClock->advanceMs(3_600_000);
        });

        self::assertSame(2, $worker->runOnce());
        self::assertCount(2, $recordingTransport->acked);
    }

    // --- configurazione --------------------------------------------------------------------------------------

    public function testTimeoutMustBeShorterThanClaimIdle(): void
    {
        $fakeAlarm = new FakeAlarm();
        new Worker($this->spyTransport, $this->queueConfig, new WorkerConfig(middlewares: [new TimeoutMiddleware($fakeAlarm, 9)]));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Handler timeout (10 s) must be shorter than claimIdleMs (10000 ms).');

        new Worker($this->spyTransport, $this->queueConfig, new WorkerConfig(middlewares: [new TimeoutMiddleware($fakeAlarm, 10)]));
    }

    public function testTimeoutJustBelowClaimIdleIsAccepted(): void
    {
        $queueConfig = new QueueConfig(claimIdleMs: 10_005);

        new Worker($this->spyTransport, $queueConfig, new WorkerConfig(middlewares: [new TimeoutMiddleware(new FakeAlarm(), 10)]));

        $this->expectException(InvalidArgumentException::class);
        new Worker($this->spyTransport, new QueueConfig(claimIdleMs: 10_000), new WorkerConfig(middlewares: [new TimeoutMiddleware(new FakeAlarm(), 10)]));
    }

    public function testDefaultsAreUsable(): void
    {
        $recordingTransport = new RecordingTransport();
        $recordingTransport->incoming[] = [$this->delivery('test', '{"ok":true}')];
        $seen = null;

        $worker = (new Worker($recordingTransport))->on('test', static function (Message $message, DecodedText $decodedText) use (&$seen): void {
            $seen = $decodedText->value;
        });

        self::assertSame(1, $worker->runOnce());
        self::assertSame(['ok' => true], $seen);
    }

    private function worker(
        ?WorkerConfig $workerConfig = null,
        ?TransportInterface $transport = null,
        ?FakeSignalHandler $signalHandler = null,
        ?FakeMemoryMeter $memoryMeter = null,
    ): Worker {
        $workerConfig ??= new WorkerConfig(listeners: [$this->recordingListener]);

        return new Worker(
            $transport ?? $this->spyTransport,
            $this->queueConfig,
            $workerConfig,
            $this->fakeClock,
            $signalHandler ?? new FakeSignalHandler(),
            $memoryMeter ?? new FakeMemoryMeter(),
            $this->recordingLogger,
        );
    }

    private function send(string $channel, string $text, ?int $ttlMs = null): void
    {
        $now = $this->fakeClock->now();
        $this->inMemoryTransport->send(new Message(
            $this->uuidV7Generator->generate(),
            $channel,
            $text,
            $now,
            expiresAt: null === $ttlMs ? null : $now->modify(\sprintf('+%d milliseconds', $ttlMs)),
        ));
    }

    private function delivery(string $channel, string $text, int $deliveryCount = 1): Delivery
    {
        $message = new Message($this->uuidV7Generator->generate(), $channel, $text, new DateTimeImmutable('2026-01-01T00:00:00Z'));

        return new Delivery($message, 'r-'.$text, $this->fakeClock->now(), $deliveryCount);
    }

    private function middleware(string $name, CallLog $callLog): MiddlewareInterface
    {
        return new readonly class($name, $callLog) implements MiddlewareInterface {
            public function __construct(private string $name, private CallLog $callLog)
            {
            }

            #[Override]
            public function process(Message $message, callable $next): void
            {
                $this->callLog->record($this->name.':before');
                $next($message);
                $this->callLog->record($this->name.':after');
            }
        };
    }
}
