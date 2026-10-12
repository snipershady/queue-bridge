<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\RabbitMq;

use DateTimeImmutable;
use Generator;
use InvalidArgumentException;
use LogicException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Codec\EnvelopeCodec;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\BatchSendException;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Exception\SerializationException;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Tests\Support\FakeAmqpChannel;
use QueueBridge\Tests\Support\FakeAmqpConnector;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Transport\DeadLetter;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueuedMessageState;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\RabbitMq\Client\AmqpMessage;
use QueueBridge\Transport\RabbitMq\RabbitMqTransport;
use QueueBridge\Transport\RabbitMq\RetryTiers;
use QueueBridge\Transport\RabbitMq\Topology;
use QueueBridge\Transport\TransportCapabilities;
use RuntimeException;

#[CoversClass(RabbitMqTransport::class)]
#[UsesClass(Topology::class)]
#[UsesClass(RetryTiers::class)]
#[UsesClass(AmqpMessage::class)]
#[UsesClass(EnvelopeCodec::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(FailureReason::class)]
#[UsesClass(DeadLetter::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(QueuedMessage::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(ConnectionException::class)]
#[UsesClass(BatchSendException::class)]
#[UsesClass(SerializationException::class)]
final class RabbitMqTransportTest extends TestCase
{
    private const string ID = '0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    private const string NOW_MS = '1767225600000';

    private FakeClock $fakeClock;

    private FakeAmqpConnector $fakeAmqpConnector;

    private FakeAmqpChannel $fakeAmqpChannel;

    private RecordingLogger $recordingLogger;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock('2026-01-01T00:00:00.000Z');
        $this->fakeAmqpConnector = new FakeAmqpConnector();
        $this->fakeAmqpChannel = $this->fakeAmqpConnector->channel;
        $this->recordingLogger = new RecordingLogger();
    }

    // --- invio ----------------------------------------------------------------------------------------------

    public function testSendPublishesToTheExchangeWithTheChannelAsRoutingKey(): void
    {
        $this->transport()->send($this->message('orders.created', '{"a":1}'));

        [$amqpMessage, $exchange, $routingKey] = $this->fakeAmqpChannel->published[0];
        self::assertSame(['rq.q', 'orders.created'], [$exchange, $routingKey]);
        self::assertSame('{"a":1}', $amqpMessage->body);
        self::assertSame(self::ID, $amqpMessage->messageId);
        self::assertSame([
            'x-qb-v' => '1',
            'x-qb-id' => self::ID,
            'x-qb-channel' => 'orders.created',
            'x-qb-attempts' => '0',
            'x-qb-created_at' => self::NOW_MS,
            'x-qb-headers' => '{}',
        ], $amqpMessage->headers);
        self::assertContains('confirm:5000', $this->fakeAmqpChannel->calls);
    }

    public function testSendWithDelayGoesToTheLargestTierNotAboveTheDelay(): void
    {
        $this->transport()->send($this->message(), 7000);
        $this->transport()->send($this->message(), 300);

        self::assertSame(['', 'rq.q.retry.5000'], [$this->fakeAmqpChannel->published[0][1], $this->fakeAmqpChannel->published[0][2]]);
        self::assertSame('1767225607000', $this->fakeAmqpChannel->published[0][0]->headers['x-qb-due-at']);
        self::assertSame('rq.q.retry.1000', $this->fakeAmqpChannel->published[1][2]);
    }

    public function testSendFailsWhenTheBrokerDoesNotConfirm(): void
    {
        $this->fakeAmqpChannel->failedConfirms[] = [self::ID];

        try {
            $this->transport()->send($this->message());
            self::fail('Expected CommandFailedException');
        } catch (CommandFailedException $commandFailedException) {
            self::assertStringContainsString('was not confirmed', $commandFailedException->getMessage());
        }

        self::assertSame(1, $this->fakeAmqpConnector->resets);
    }

    public function testSendRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->transport()->send($this->message(), -1);
    }

    public function testSendBatchConfirmsOnce(): void
    {
        $this->transport()->sendBatch([new Envelope($this->message()), new Envelope($this->message('test', 'b', '0190f2a4-5b6c-7d8e-9f01-000000000002'), 1000)]);
        $this->transport()->sendBatch([]);

        self::assertCount(2, $this->fakeAmqpChannel->published);
        self::assertSame(['confirm:5000'], array_values(array_filter($this->fakeAmqpChannel->calls, static fn (string $call): bool => \str_starts_with($call, 'confirm'))));
    }

    public function testSendBatchReportsUnconfirmedMessagesWithoutAbandoningTheChannel(): void
    {
        $first = $this->message();
        $second = $this->message('test', 'b', '0190f2a4-5b6c-7d8e-9f01-000000000002');
        $this->fakeAmqpChannel->failedConfirms[] = [$second->id->value];

        try {
            $this->transport()->sendBatch([new Envelope($first), new Envelope($second)]);
            self::fail('Expected BatchSendException');
        } catch (BatchSendException $batchSendException) {
            self::assertSame([$first->id], $batchSendException->sentIds);
            self::assertSame([$second->id], $batchSendException->failedIds);
        }

        self::assertSame(0, $this->fakeAmqpConnector->resets);
    }

    public function testSendBatchPublishesNothingWhenTheIterableFails(): void
    {
        $message = $this->message();

        try {
            $this->transport()->sendBatch((static function () use ($message): Generator {
                yield new Envelope($message);

                throw new RuntimeException('source failed');
            })());
            self::fail('Expected RuntimeException');
        } catch (RuntimeException $runtimeException) {
            self::assertSame('source failed', $runtimeException->getMessage());
        }

        self::assertSame([[], []], [$this->fakeAmqpChannel->published, $this->fakeAmqpChannel->calls]);
    }

    // --- ricezione ------------------------------------------------------------------------------------------

    public function testReceiveStartsConsumingOnceAndWaitsOnlyWhenTheBufferIsEmpty(): void
    {
        $rabbitMqTransport = $this->transport(prefetch: 3);
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2)], [$this->amqp(3)]];

        $first = $rabbitMqTransport->receive(10, 2000);
        $second = $rabbitMqTransport->receive(10, 2000);

        self::assertSame(['1:1', '1:2', '1:3'], array_map(static fn (Delivery $delivery): string => $delivery->receipt, $first));
        self::assertSame([], $second);
        self::assertSame(['prefetch:3', 'consume:rq.q'], array_values(array_filter($this->fakeAmqpChannel->calls, static fn (string $call): bool => \str_starts_with($call, 'prefetch') || \str_starts_with($call, 'consume'))));
        self::assertSame([2000, 0, 0, 2000], $this->fakeAmqpChannel->waits);
    }

    public function testReceiveHonoursMaxAndKeepsTheRestBuffered(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2), $this->amqp(3)]];

        self::assertCount(2, $rabbitMqTransport->receive(2, 100));
        self::assertCount(1, $rabbitMqTransport->receive(2, 100));
        // Seconda ricezione: buffer non vuoto, quindi nessuna attesa bloccante, solo una lettura immediata.
        self::assertSame([100, 0], $this->fakeAmqpChannel->waits);
    }

    public function testDeliveryCountComesFromTheBrokerHeader(): void
    {
        $this->fakeAmqpChannel->incoming = [[$this->amqp(7, ['x-delivery-count' => 2])]];

        [$delivery] = $this->transport()->receive();

        self::assertSame(3, $delivery->deliveryCount);
        self::assertSame(self::ID, $delivery->message->id->value);
        self::assertSame('x', $delivery->message->text);
    }

    public function testMessageBackFromATierBeforeItsDueTimeIsSentBackToWait(): void
    {
        $this->fakeAmqpChannel->incoming = [[$this->amqp(7, ['x-qb-due-at' => '1767225602500'])]];

        self::assertSame([], $this->transport()->receive());

        [$amqpMessage, $exchange, $routingKey] = $this->fakeAmqpChannel->published[0];
        self::assertSame(['', 'rq.q.retry.1000', '1767225602500'], [$exchange, $routingKey, $amqpMessage->headers['x-qb-due-at']]);
        self::assertSame([7], $this->fakeAmqpChannel->acked);
    }

    public function testEarlyMessageIsNotAcknowledgedWhenItsWaitIsNotConfirmed(): void
    {
        $this->fakeAmqpChannel->incoming = [[$this->amqp(7, ['x-qb-due-at' => '1767225602500'])]];
        $this->fakeAmqpChannel->failedConfirms[] = [self::ID];

        try {
            $this->transport()->receive();
            self::fail('Expected CommandFailedException');
        } catch (CommandFailedException) {
            self::assertSame([], $this->fakeAmqpChannel->acked);
        }
    }

    public function testUndecodableMessageIsNotAcknowledgedWhenTheDeadLetterCopyIsNotConfirmed(): void
    {
        $this->fakeAmqpChannel->incoming = [[new AmqpMessage('body', ['x-qb-v' => '9'], 'm-1', 7)]];
        $this->fakeAmqpChannel->failedConfirms[] = ['m-1'];

        try {
            $this->transport()->receive();
            self::fail('Expected CommandFailedException');
        } catch (CommandFailedException) {
            self::assertSame([], $this->fakeAmqpChannel->acked);
        }
    }

    public function testDefaults(): void
    {
        $rabbitMqTransport = new RabbitMqTransport($this->fakeAmqpConnector, new QueueConfig(name: 'q'), $this->fakeClock);
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = array_map(fn (int $tag): AmqpMessage => $this->amqp($tag), range(1, 101));

        $rabbitMqTransport->receive();

        self::assertContains('prefetch:10', $this->fakeAmqpChannel->calls);
        self::assertSame([1000], $this->fakeAmqpChannel->waits);
        self::assertCount(100, $rabbitMqTransport->deadLetters());

        // Il canale finto non rimette in coda su nack: si ricarica la DLQ.
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = array_map(fn (int $tag): AmqpMessage => $this->amqp($tag), range(1, 101));
        self::assertSame(100, $rabbitMqTransport->requeueDeadLetters());
        self::assertSame('rq.q.retry.3600000', (new Topology(new QueueConfig(name: 'q'), new RetryTiers()))->retryQueues()[5]);
    }

    public function testBoundaryArgumentsAreAccepted(): void
    {
        $rabbitMqTransport = new RabbitMqTransport($this->fakeAmqpConnector, new QueueConfig(name: 'q'), $this->fakeClock, prefetch: 1, confirmTimeoutMs: 1);

        self::assertSame([], $rabbitMqTransport->receive(1, 0));
        self::assertSame([0], $this->fakeAmqpChannel->waits);
        self::assertSame([], $rabbitMqTransport->deadLetters(1));
    }

    public function testDueMessageIsDelivered(): void
    {
        $this->fakeAmqpChannel->incoming = [[$this->amqp(7, ['x-qb-due-at' => self::NOW_MS])]];

        self::assertCount(1, $this->transport()->receive());
    }

    public function testUndecodableMessageGoesToTheDeadLetterQueue(): void
    {
        $this->fakeAmqpChannel->incoming = [[new AmqpMessage('body', ['x-qb-v' => '9', 'other' => 1], 'm-1', 7)]];

        self::assertSame([], $this->transport()->receive());

        [$amqpMessage, $exchange, $routingKey] = $this->fakeAmqpChannel->published[0];
        self::assertSame(['', 'rq.q.dlq', 'body', 'm-1'], [$exchange, $routingKey, $amqpMessage->body, $amqpMessage->messageId]);
        self::assertSame([
            'x-qb-v' => '9',
            'other' => 1,
            'x-qb-failure-kind' => 'serialization',
            'x-qb-failure-error-class' => SerializationException::class,
            'x-qb-failure-message' => 'Unsupported wire format version "9".',
            'x-qb-failure-at' => self::NOW_MS,
        ], $amqpMessage->headers);
        self::assertSame([7], $this->fakeAmqpChannel->acked);
        self::assertSame(['error'], $this->recordingLogger->levels());
    }

    public function testDeliveryWithoutTagIsAProtocolError(): void
    {
        $this->fakeAmqpChannel->incoming = [[new AmqpMessage('x', $this->headers())]];

        $this->expectException(CommandFailedException::class);

        $this->transport()->receive();
    }

    public function testReceiveValidatesItsArguments(): void
    {
        foreach ([[0, 0], [1, -1]] as [$max, $timeout]) {
            try {
                $this->transport()->receive($max, $timeout);
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertStringContainsString('must be >= ', $invalidArgumentException->getMessage());
            }
        }
    }

    public function testConnectionErrorAbandonsTheChannelAndConsumingRestarts(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1)]];
        [$stale] = $rabbitMqTransport->receive();

        $this->fakeAmqpChannel->failNextPublish = ConnectionException::unreachable('RabbitMQ', 'broken pipe');
        try {
            $rabbitMqTransport->send($this->message());
            self::fail('Expected ConnectionException');
        } catch (ConnectionException) {
            self::assertSame(1, $this->fakeAmqpConnector->resets);
        }

        $rabbitMqTransport->ack($stale);
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1)]];
        [$fresh] = $rabbitMqTransport->receive();

        self::assertSame([], $this->fakeAmqpChannel->acked);
        self::assertSame('2:1', $fresh->receipt);
        self::assertCount(2, array_filter($this->fakeAmqpChannel->calls, static fn (string $call): bool => \str_starts_with($call, 'consume')));
    }

    // --- conferma, retry, reject, release ------------------------------------------------------------------------

    public function testAckOnlyConfirmsDeliveriesStillOwned(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2)]];
        [$first, $second] = $rabbitMqTransport->receive(2);

        $rabbitMqTransport->ackBatch([$first, $second]);
        $rabbitMqTransport->ack($first);
        $rabbitMqTransport->ack(new Delivery($first->message, 'garbage', $this->fakeClock->now()));
        $rabbitMqTransport->ack(new Delivery($first->message, '9:1', $this->fakeClock->now()));

        self::assertSame([1, 2], $this->fakeAmqpChannel->acked);
    }

    public function testMalformedReceiptsHaveNoEffect(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);
        [$generation] = explode(':', $delivery->receipt);

        foreach ([$generation.':1:2', $generation.':', $generation.':-1', $generation.': 1'] as $receipt) {
            $rabbitMqTransport->ack(new Delivery($delivery->message, $receipt, $this->fakeClock->now()));
        }

        self::assertSame([], $this->fakeAmqpChannel->acked);
    }

    public function testDeliveriesOfAChannelClosedByTheBrokerAreNotSettledOnTheReopenedOne(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2), $this->amqp(3)]];
        [$first, $second, $third] = $rabbitMqTransport->receive(3);

        // consumer_timeout o coda cancellata: il broker chiude il canale e rimette in coda i messaggi non confermati.
        $this->fakeAmqpChannel->open = false;
        $rabbitMqTransport->ack($first);
        $rabbitMqTransport->retry($second, 0);
        $rabbitMqTransport->release($third);

        self::assertSame([[], [], []], [$this->fakeAmqpChannel->acked, $this->fakeAmqpChannel->published, $this->fakeAmqpChannel->nacked]);
        self::assertTrue($this->fakeAmqpChannel->open, 'the channel was reopened');
    }

    public function testRetryNowPublishesTheNextAttemptToTheQueueThenAcknowledges(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);

        $rabbitMqTransport->retry($delivery, 0);

        [$amqpMessage, $exchange, $routingKey] = $this->fakeAmqpChannel->published[0];
        self::assertSame(['', 'rq.q', '1'], [$exchange, $routingKey, $amqpMessage->headers['x-qb-attempts']]);
        self::assertSame([5], $this->fakeAmqpChannel->acked);
        self::assertSame(0, $rabbitMqTransport->stats()->inFlight);
    }

    public function testRetryLaterGoesThroughATier(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);

        $rabbitMqTransport->retry($delivery, 30_000);

        self::assertSame('rq.q.retry.30000', $this->fakeAmqpChannel->published[0][2]);
        self::assertSame('1767225630000', $this->fakeAmqpChannel->published[0][0]->headers['x-qb-due-at']);
    }

    public function testRetryDoesNotAcknowledgeWhenTheCopyIsNotConfirmed(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);
        $this->fakeAmqpChannel->failedConfirms[] = [self::ID];

        try {
            $rabbitMqTransport->retry($delivery, 0);
            self::fail('Expected CommandFailedException');
        } catch (CommandFailedException) {
            self::assertSame([], $this->fakeAmqpChannel->acked);
        }
    }

    public function testRetryRejectAndReleaseOfAnUnknownDeliveryDoNothing(): void
    {
        $rabbitMqTransport = $this->transport();
        $ghost = new Delivery($this->message(), '1:42', $this->fakeClock->now());

        $rabbitMqTransport->retry($ghost, 0);
        $rabbitMqTransport->reject($ghost, new FailureReason(FailureKind::Expired, $ghost->message->id, $this->fakeClock->now()));
        $rabbitMqTransport->release($ghost);

        self::assertSame([[], [], []], [$this->fakeAmqpChannel->published, $this->fakeAmqpChannel->acked, $this->fakeAmqpChannel->nacked]);
    }

    public function testRetryRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->transport()->retry(new Delivery($this->message(), '1:1', $this->fakeClock->now()), -1);
    }

    public function testRejectPublishesToTheDeadLetterQueueWithTheReason(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);

        $rabbitMqTransport->reject($delivery, FailureReason::fromThrowable(FailureKind::HandlerFailed, $delivery->message->id, $this->fakeClock->now(), new LogicException('boom')));
        $rabbitMqTransport->reject($delivery, new FailureReason(FailureKind::Expired, $delivery->message->id, $this->fakeClock->now()));

        [$amqpMessage, $exchange, $routingKey] = $this->fakeAmqpChannel->published[0];
        self::assertSame(['', 'rq.q.dlq'], [$exchange, $routingKey]);
        self::assertSame(
            ['handler_failed', LogicException::class, 'boom', self::NOW_MS],
            [$amqpMessage->headers['x-qb-failure-kind'], $amqpMessage->headers['x-qb-failure-error-class'], $amqpMessage->headers['x-qb-failure-message'], $amqpMessage->headers['x-qb-failure-at']],
        );
        self::assertSame([5], $this->fakeAmqpChannel->acked);
        self::assertCount(1, $this->fakeAmqpChannel->published, 'second reject: delivery no longer owned');
    }

    public function testRejectWithoutExceptionStoresAnEmptyErrorClass(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);

        $rabbitMqTransport->reject($delivery, new FailureReason(FailureKind::Expired, $delivery->message->id, $this->fakeClock->now()));

        self::assertSame('', $this->fakeAmqpChannel->published[0][0]->headers['x-qb-failure-error-class']);
    }

    public function testReleaseRequeuesTheDelivery(): void
    {
        $rabbitMqTransport = $this->transport();
        [$delivery] = $this->received($rabbitMqTransport);

        $rabbitMqTransport->release($delivery);
        $rabbitMqTransport->release($delivery);

        self::assertSame([[5, true]], $this->fakeAmqpChannel->nacked);
    }

    // --- setup, ping, capabilities, close ------------------------------------------------------------------------

    public function testSetupDeclaresTheTopology(): void
    {
        $this->transport(retryTiers: new RetryTiers([1000, 5000]))->setup();

        self::assertSame([['rq.q', 'topic']], $this->fakeAmqpChannel->declaredExchanges);
        self::assertSame(['rq.q', 'rq.q.dlq', 'rq.q.retry.1000', 'rq.q.retry.5000'], array_column($this->fakeAmqpChannel->declaredQueues, 0));
        self::assertSame([['rq.q', 'rq.q', '#']], $this->fakeAmqpChannel->bindings);
    }

    public function testPingOpensTheChannel(): void
    {
        $this->transport()->ping();

        self::assertSame(1, $this->fakeAmqpConnector->generation());
    }

    public function testCapabilities(): void
    {
        $transportCapabilities = $this->transport()->capabilities();

        self::assertSame(OrphanRecovery::Native, $transportCapabilities->orphanRecovery);
        self::assertTrue($transportCapabilities->nativeDelay && $transportCapabilities->batchReceive && $transportCapabilities->batchAck && $transportCapabilities->deadLetter && $transportCapabilities->deliveryCount);
        self::assertFalse($transportCapabilities->consumerLag);
    }

    public function testCloseForgetsBufferedAndHandedOutMessages(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2)]];
        [$delivery] = $rabbitMqTransport->receive(1);

        $rabbitMqTransport->close();
        $rabbitMqTransport->ack($delivery);

        self::assertSame(1, $this->fakeAmqpConnector->resets);
        self::assertSame([], $this->fakeAmqpChannel->acked);
        self::assertSame([0, 0], [$rabbitMqTransport->stats()->ready, $rabbitMqTransport->stats()->inFlight]);
    }

    public function testConstructorValidatesPrefetchAndConfirmTimeout(): void
    {
        foreach ([[0, 1], [1, 0]] as [$prefetch, $confirmTimeoutMs]) {
            try {
                new RabbitMqTransport($this->fakeAmqpConnector, new QueueConfig(name: 'q'), $this->fakeClock, prefetch: $prefetch, confirmTimeoutMs: $confirmTimeoutMs);
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertStringContainsString('must be >= 1', $invalidArgumentException->getMessage());
            }
        }
    }

    // --- statistiche ----------------------------------------------------------------------------------------------

    public function testStatsCountBrokerQueuesAndLocalDeliveries(): void
    {
        $rabbitMqTransport = $this->transport(retryTiers: new RetryTiers([1000, 5000]));
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1)], [$this->amqp(2)]];
        $rabbitMqTransport->receive(1);
        $this->fakeAmqpChannel->queueInfo = ['rq.q' => [4, 2], 'rq.q.retry.1000' => [1, 0], 'rq.q.retry.5000' => [2, 0], 'rq.q.dlq' => [3, 0]];

        $queueStats = $rabbitMqTransport->stats();

        self::assertSame([5, 1, 3, 3, null, 2], [$queueStats->ready, $queueStats->inFlight, $queueStats->delayed, $queueStats->deadLetters, $queueStats->lag, $queueStats->consumers]);
        self::assertSame(['rq.q', 'rq.q.retry.1000', 'rq.q.retry.5000', 'rq.q.dlq'], array_column($this->fakeAmqpChannel->declaredQueues, 0));
        self::assertSame([true, true, true, true], array_column($this->fakeAmqpChannel->declaredQueues, 2));
    }

    public function testStatsBeforeSetupAbandonTheClosedChannel(): void
    {
        $this->fakeAmqpChannel->queueInfo = ['rq.q' => CommandFailedException::rejected('queue.declare', 'NOT_FOUND')];

        $queueStats = $this->transport()->stats();

        self::assertSame([0, 0, 0, 0], [$queueStats->ready, $queueStats->inFlight, $queueStats->delayed, $queueStats->deadLetters]);
        self::assertSame(1, $this->fakeAmqpConnector->resets);
    }

    // --- dead letter ----------------------------------------------------------------------------------------------

    public function testDeadLettersPeekAndReturnEveryMessage(): void
    {
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = [
            $this->amqp(1, ['x-qb-failure-kind' => 'unrecoverable', 'x-qb-failure-error-class' => LogicException::class, 'x-qb-failure-message' => 'bad', 'x-qb-failure-at' => '1767225600123']),
            new AmqpMessage('corrupt', ['x-qb-v' => '1'], 'c', 2),
            $this->amqp(3, ['x-qb-failure-kind' => 'mystery']),
            $this->amqp(4),
        ];

        $deadLetters = $this->transport()->deadLetters(3);

        self::assertCount(2, $deadLetters);
        self::assertSame([FailureKind::Unrecoverable, LogicException::class, 'bad'], [$deadLetters[0]->reason->kind, $deadLetters[0]->reason->errorClass, $deadLetters[0]->reason->errorMessage]);
        self::assertSame('2026-01-01T00:00:00.123+00:00', $deadLetters[0]->reason->failedAt->format('Y-m-d\TH:i:s.vP'));
        self::assertSame([FailureKind::HandlerFailed, null, ''], [$deadLetters[1]->reason->kind, $deadLetters[1]->reason->errorClass, $deadLetters[1]->reason->errorMessage]);
        self::assertSame([[1, true], [2, true], [3, true]], $this->fakeAmqpChannel->nacked);
        self::assertCount(1, $this->fakeAmqpChannel->queues['rq.q.dlq']);
    }

    public function testRequeueDeadLettersRepublishesWithZeroAttemptsAndWithoutTheReason(): void
    {
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = [
            $this->amqp(1, ['x-qb-attempts' => '3', 'x-qb-failure-kind' => 'handler_failed']),
            new AmqpMessage('corrupt', [], 'c', 2),
        ];

        self::assertSame(1, $this->transport()->requeueDeadLetters());

        [$amqpMessage, $exchange, $routingKey] = $this->fakeAmqpChannel->published[0];
        self::assertSame(['', 'rq.q', '0'], [$exchange, $routingKey, $amqpMessage->headers['x-qb-attempts']]);
        self::assertArrayNotHasKey('x-qb-failure-kind', $amqpMessage->headers);

        // Il messaggio non decodificabile resta in DLQ, ma in fondo: copia confermata, poi ack dell'originale.
        [$corrupt, $exchange, $routingKey] = $this->fakeAmqpChannel->published[1];
        self::assertSame(['', 'rq.q.dlq', 'corrupt', [], 'c'], [$exchange, $routingKey, $corrupt->body, $corrupt->headers, $corrupt->messageId]);
        self::assertSame([1, 2], $this->fakeAmqpChannel->acked);
        self::assertSame([], $this->fakeAmqpChannel->nacked);
    }

    public function testRequeueDeadLettersProgressesPastAHeadOfUndecodableMessages(): void
    {
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = [new AmqpMessage('corrupt 1', [], 'c1', 1), new AmqpMessage('corrupt 2', [], 'c2', 2)];

        self::assertSame(0, $this->transport()->requeueDeadLetters(2));
        self::assertSame(['rq.q.dlq', 'rq.q.dlq'], array_map(static fn (array $published): string => $published[2], $this->fakeAmqpChannel->published));
        self::assertSame([1, 2], $this->fakeAmqpChannel->acked);
        self::assertContains('confirm:5000', $this->fakeAmqpChannel->calls);
    }

    public function testRequeueDeadLettersDoesNotAcknowledgeWhenTheCopiesAreNotConfirmed(): void
    {
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = [$this->amqp(1), new AmqpMessage('corrupt', [], 'c', 2)];
        $this->fakeAmqpChannel->failedConfirms[] = ['c'];

        try {
            $this->transport()->requeueDeadLetters();
            self::fail('Expected CommandFailedException');
        } catch (CommandFailedException $commandFailedException) {
            self::assertStringContainsString('message c was not confirmed', $commandFailedException->getMessage());
        }

        self::assertSame([], $this->fakeAmqpChannel->acked);
        self::assertSame(1, $this->fakeAmqpConnector->resets, 'the channel is abandoned: the broker gives the messages back');
    }

    public function testDeadLetterOperationsBeforeSetupAbandonTheClosedChannel(): void
    {
        $this->fakeAmqpChannel->queueInfo['rq.q.dlq'] = CommandFailedException::rejected('queue.declare', "NOT_FOUND - no queue 'rq.q.dlq'");
        $rabbitMqTransport = $this->transport();

        self::assertSame([], $rabbitMqTransport->deadLetters());
        self::assertSame(0, $rabbitMqTransport->requeueDeadLetters());
        self::assertFalse($rabbitMqTransport->requeueDeadLetter(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-000000000001')));
        self::assertSame(0, $rabbitMqTransport->purgeDeadLetters());

        self::assertSame(4, $this->fakeAmqpConnector->resets);
        self::assertSame([], array_values(array_filter($this->fakeAmqpChannel->calls, static fn (string $call): bool => \str_starts_with($call, 'get:') || \str_starts_with($call, 'purgeQueue:'))));
    }

    public function testRequeueOfAnEmptyDeadLetterQueue(): void
    {
        self::assertSame(0, $this->transport()->requeueDeadLetters(5));
        self::assertNotContains('confirm:5000', $this->fakeAmqpChannel->calls);
    }

    public function testRequeueSingleDeadLetter(): void
    {
        $target = '0190f2a4-5b6c-7d8e-9f01-000000000009';
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = [
            $this->amqp(1),
            new AmqpMessage('corrupt twin', [], $target, 2),
            $this->amqp(3, [], $target),
            $this->amqp(4),
        ];

        self::assertTrue($this->transport()->requeueDeadLetter(MessageId::fromString($target)));
        self::assertSame([3], $this->fakeAmqpChannel->acked);
        self::assertSame([[1, true], [2, true]], $this->fakeAmqpChannel->nacked);

        self::assertFalse($this->transport()->requeueDeadLetter(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-000000000099')));
    }

    public function testDeadLetterWithoutTagIsAProtocolError(): void
    {
        $this->fakeAmqpChannel->queues['rq.q.dlq'] = [new AmqpMessage('x', $this->headers())];

        $this->expectException(CommandFailedException::class);

        $this->transport()->deadLetters();
    }

    public function testPurgeDeadLetters(): void
    {
        $this->fakeAmqpChannel->purged = 6;

        self::assertSame(6, $this->transport()->purgeDeadLetters());
        self::assertContains('purgeQueue:rq.q.dlq', $this->fakeAmqpChannel->calls);
    }

    public function testDeadLetterLimitsMustBePositive(): void
    {
        foreach ([fn (): array => $this->transport()->deadLetters(0), fn (): int => $this->transport()->requeueDeadLetters(0), fn (): array => $this->transport()->browse(0)] as $call) {
            try {
                $call();
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertSame('Limit must be >= 1, got 0.', $invalidArgumentException->getMessage());
            }
        }
    }

    // --- svuotamento ed elenco ----------------------------------------------------------------------------------

    public function testPurgeEmptiesTheMainQueueAndTheWaitingQueues(): void
    {
        $this->fakeAmqpChannel->purged = 2;

        self::assertSame(6, $this->transport(retryTiers: new RetryTiers([1000, 5000]))->purge());
        self::assertSame(['purgeQueue:rq.q', 'purgeQueue:rq.q.retry.1000', 'purgeQueue:rq.q.retry.5000'], $this->fakeAmqpChannel->calls);
        self::assertSame([], $this->fakeAmqpChannel->acked);
    }

    public function testPurgeAlsoDropsTheMessagesPrefetchedByThisConsumer(): void
    {
        $rabbitMqTransport = $this->transport(retryTiers: new RetryTiers([1000]));
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2), $this->amqp(3)]];
        [$inFlight] = $rabbitMqTransport->receive(1);
        $this->fakeAmqpChannel->incoming = [[$this->amqp(4)]];

        self::assertSame(3, $rabbitMqTransport->purge());
        self::assertSame([2, 3, 4], $this->fakeAmqpChannel->acked, 'buffered messages are confirmed, the delivery in flight is not');
        self::assertSame([], $rabbitMqTransport->receive(10, 0));

        $rabbitMqTransport->ack($inFlight);
        self::assertSame([2, 3, 4, 1], $this->fakeAmqpChannel->acked);
    }

    public function testPurgeBeforeSetupAbandonsTheClosedChannel(): void
    {
        $this->fakeAmqpChannel->failPurge = CommandFailedException::rejected('queue.purge', 'NOT_FOUND');

        self::assertSame(0, $this->transport()->purge());
        self::assertSame(1, $this->fakeAmqpConnector->resets);
    }

    public function testBrowseListsPrefetchedThenBrokerMessagesAndGivesThemBack(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2)]];
        $rabbitMqTransport->receive(1);
        $this->fakeAmqpChannel->queues['rq.q'] = [
            $this->amqp(10),
            new AmqpMessage('corrupt', ['x-qb-v' => '1'], 'c', 11),
            $this->amqp(12, ['x-qb-due-at' => '1767225602500']),
            $this->amqp(13, ['x-qb-due-at' => '1767225599000']),
        ];

        $queued = $rabbitMqTransport->browse(10);

        self::assertSame(
            [QueuedMessageState::Ready, QueuedMessageState::Ready, QueuedMessageState::Delayed, QueuedMessageState::Ready],
            array_map(static fn (QueuedMessage $queuedMessage): QueuedMessageState => $queuedMessage->state, $queued),
        );
        self::assertSame('2026-01-01T00:00:02.500+00:00', $queued[2]->availableAt?->format('Y-m-d\TH:i:s.vP'));
        self::assertSame([[10, true], [11, true], [12, true], [13, true]], $this->fakeAmqpChannel->nacked);
        self::assertSame(['1:2'], array_map(static fn (Delivery $delivery): string => $delivery->receipt, $rabbitMqTransport->receive(10, 0)), 'the prefetched message is still buffered');
    }

    public function testBrowseStopsAtTheLimit(): void
    {
        $this->fakeAmqpChannel->queues['rq.q'] = [$this->amqp(1), $this->amqp(2), $this->amqp(3)];

        self::assertCount(2, $this->transport()->browse(2));
        self::assertSame([[1, true], [2, true]], $this->fakeAmqpChannel->nacked);
        self::assertCount(1, $this->fakeAmqpChannel->queues['rq.q']);
    }

    public function testBrowseLimitCanBeFilledByPrefetchedMessages(): void
    {
        $rabbitMqTransport = $this->transport();
        $this->fakeAmqpChannel->incoming = [[$this->amqp(1), $this->amqp(2), $this->amqp(3)]];
        $rabbitMqTransport->receive(1);
        $this->fakeAmqpChannel->queues['rq.q'] = [$this->amqp(10)];

        self::assertCount(1, $rabbitMqTransport->browse(1));
        self::assertNotContains('get:rq.q', $this->fakeAmqpChannel->calls);
    }

    public function testBrowseBeforeSetupAbandonsTheClosedChannel(): void
    {
        $this->fakeAmqpChannel->failGet = CommandFailedException::rejected('basic.get', 'NOT_FOUND');

        self::assertSame([], $this->transport()->browse());
        self::assertSame(1, $this->fakeAmqpConnector->resets);
    }

    private function transport(int $prefetch = 10, RetryTiers $retryTiers = new RetryTiers()): RabbitMqTransport
    {
        return new RabbitMqTransport(
            $this->fakeAmqpConnector,
            new QueueConfig(name: 'q'),
            $this->fakeClock,
            $retryTiers,
            $prefetch,
            logger: $this->recordingLogger,
        );
    }

    /**
     * @return list<Delivery>
     */
    private function received(RabbitMqTransport $rabbitMqTransport): array
    {
        $this->fakeAmqpChannel->incoming = [[$this->amqp(5)]];

        return $rabbitMqTransport->receive();
    }

    private function message(string $channel = 'test', string $text = 'x', string $id = self::ID): Message
    {
        return new Message(MessageId::fromString($id), $channel, $text, new DateTimeImmutable('2026-01-01T00:00:00Z'));
    }

    /**
     * @param array<string, string|int> $extra
     */
    private function amqp(int $tag, array $extra = [], string $id = self::ID): AmqpMessage
    {
        return new AmqpMessage('x', [...$this->headers($id), ...$extra], $id, $tag);
    }

    /**
     * @return array<string, string>
     */
    private function headers(string $id = self::ID): array
    {
        return [
            'x-qb-v' => '1',
            'x-qb-id' => $id,
            'x-qb-channel' => 'test',
            'x-qb-attempts' => '0',
            'x-qb-created_at' => self::NOW_MS,
            'x-qb-headers' => '{}',
        ];
    }
}
