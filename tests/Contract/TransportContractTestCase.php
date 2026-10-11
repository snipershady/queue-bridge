<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Contract;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Transport\Capability\BatchAckAwareInterface;
use QueueBridge\Transport\Capability\BrowseAwareInterface;
use QueueBridge\Transport\Capability\DeadLetterAwareInterface;
use QueueBridge\Transport\Capability\MaintenanceAwareInterface;
use QueueBridge\Transport\Capability\PurgeAwareInterface;
use QueueBridge\Transport\Capability\SetupAwareInterface;
use QueueBridge\Transport\Capability\StatsAwareInterface;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueuedMessageState;
use QueueBridge\Transport\TransportInterface;
use RuntimeException;

/**
 * Scenari che ogni backend deve superare con la stessa sequenza di eventi osservabili.
 *
 * Le sottoclassi forniscono il transport, un secondo consumer della stessa coda e il passare del tempo.
 */
abstract class TransportContractTestCase extends TestCase
{
    protected const int CLAIM_IDLE_MS = 1000;

    private ?UuidV7Generator $uuidV7Generator = null;

    final public function testSendReceiveAck(): void
    {
        $transport = $this->transport();
        $message = $this->message('test', '{"a":1}', ['correlation_id' => 'c-1']);

        $transport->send($message);
        $deliveries = $this->receive($transport, 10);

        self::assertCount(1, $deliveries);
        $this->assertSameMessage($message, $deliveries[0]->message);
        self::assertSame(1, $deliveries[0]->deliveryCount);
        self::assertSame(0, $deliveries[0]->message->attempts);

        $transport->ack($deliveries[0]);

        self::assertSame([], $this->receive($transport, 10));
    }

    final public function testBatchSendAndReceiveKeepArrivalOrder(): void
    {
        $transport = $this->transport();
        $messages = [];
        for ($i = 0; $i < 5; ++$i) {
            $messages[] = $this->message('test', 'm'.$i);
        }

        $transport->sendBatch(array_map(static fn (Message $message): Envelope => new Envelope($message), $messages));

        $received = $this->drain($transport);

        self::assertSame(['m0', 'm1', 'm2', 'm3', 'm4'], array_map(static fn (Delivery $delivery): string => $delivery->message->text, $received));
    }

    final public function testReceiveHonoursMax(): void
    {
        $transport = $this->transport();
        foreach (['a', 'b', 'c'] as $text) {
            $transport->send($this->message('test', $text));
        }

        $first = $this->receive($transport, 2);

        self::assertLessThanOrEqual(2, \count($first));
        self::assertGreaterThanOrEqual(1, \count($first));
    }

    final public function testBatchAck(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof BatchAckAwareInterface) {
            self::markTestSkipped('Transport without batch ack.');
        }

        $transport->send($this->message('test', 'a'));
        $transport->send($this->message('test', 'b'));

        $deliveries = [];
        while ([] !== ($batch = $this->receive($transport, 10))) {
            array_push($deliveries, ...$batch);
        }

        self::assertCount(2, $deliveries);
        $transport->ackBatch($deliveries);

        $this->crash($transport);
        self::assertSame([], $this->receive($transport, 10));
    }

    final public function testUnacknowledgedMessageIsRecoveredWithIncreasingDeliveryCount(): void
    {
        $transport = $this->transport();
        $message = $this->message('test', 'crash me');
        $transport->send($message);

        // Il worker riceve e "crasha" tre volte: nessun ack, nessuna eccezione gestita.
        for ($delivery = 1; $delivery <= 3; ++$delivery) {
            $deliveries = $this->receive($transport, 1);
            self::assertCount(1, $deliveries, \sprintf('delivery #%d', $delivery));
            self::assertTrue($deliveries[0]->message->id->equals($message->id));
            self::assertSame($delivery, $deliveries[0]->deliveryCount);
            self::assertSame(0, $deliveries[0]->message->attempts);

            $this->crash($transport);
        }
    }

    final public function testOrphanIsNotRecoveredBeforeClaimIdle(): void
    {
        $transport = $this->transport();
        if (OrphanRecovery::Native === $transport->capabilities()->orphanRecovery) {
            self::markTestSkipped('Orphans are recovered when the consumer disconnects.');
        }

        $transport->send($this->message('test', 'slow'));
        $this->receive($transport, 1);

        $this->maintain($transport, self::CLAIM_IDLE_MS / 4);

        self::assertSame([], $this->receive($transport, 1));
    }

    final public function testOperationsOnARecoveredDeliveryHaveNoEffect(): void
    {
        $transport = $this->transport();
        $transport->send($this->message('test', 'reclaimed'));
        [$stale] = $this->receive($transport, 1);

        $this->crash($transport);
        $consumer = $this->createConsumer($transport);
        [$current] = $this->receive($consumer, 1);

        $transport->ack($stale);
        $transport->release($stale);

        // L'ack della vecchia consegna non ha confermato quella nuova: dopo claimIdle torna di nuovo.
        $this->crash($consumer);
        $again = $this->receive($consumer, 1);
        self::assertCount(1, $again);
        self::assertSame(3, $again[0]->deliveryCount);

        $consumer->ack($again[0]);
        $consumer->ack($current);
        $this->crash($consumer);
        self::assertSame([], $this->receive($consumer, 10));
    }

    final public function testRetryRequeuesWithIncrementedAttemptsAndFreshDeliveryCount(): void
    {
        $transport = $this->transport();
        $message = $this->message('test', 'retry me');
        $transport->send($message);

        [$first] = $this->receive($transport, 1);
        $transport->retry($first, 0);

        [$second] = $this->receive($transport, 1);
        self::assertTrue($second->message->id->equals($message->id));
        self::assertSame(1, $second->message->attempts);
        self::assertSame(1, $second->deliveryCount);

        $transport->ack($second);
        $this->crash($transport);
        self::assertSame([], $this->receive($transport, 1));
    }

    final public function testRetryWithDelayIsReleasedOnlyAfterTheDelay(): void
    {
        $transport = $this->transport();
        $transport->send($this->message('test', 'later'));
        [$delivery] = $this->receive($transport, 1);

        $transport->retry($delivery, 3000);

        $this->maintain($transport, 500);
        self::assertSame([], $this->receive($transport, 1));

        $this->maintain($transport, 3000);
        $deliveries = $this->receive($transport, 1);
        self::assertCount(1, $deliveries);
        self::assertSame(1, $deliveries[0]->message->attempts);
    }

    final public function testDelayedMessageIsReleasedOnlyAfterTheDelay(): void
    {
        $transport = $this->transport();
        $transport->send($this->message('test', 'delayed'), 2000);
        $transport->send($this->message('test', 'now'));

        self::assertSame(['now'], $this->texts($this->drain($transport)));

        $this->maintain($transport, 2500);

        self::assertSame(['delayed'], $this->texts($this->drain($transport)));
    }

    final public function testRejectMovesToDeadLetterQueueAndRequeueResetsAttempts(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof DeadLetterAwareInterface) {
            self::markTestSkipped('Transport without dead letter queue.');
        }

        $message = $this->message('test', 'poison');
        $transport->send($message);
        [$first] = $this->receive($transport, 1);
        $transport->retry($first, 0);
        [$second] = $this->receive($transport, 1);

        $transport->reject($second, FailureReason::fromThrowable(
            FailureKind::HandlerFailed,
            $message->id,
            $this->clock()->now(),
            new RuntimeException('handler exploded'),
        ));

        self::assertSame([], $this->receive($transport, 1));
        $deadLetters = $transport->deadLetters();
        self::assertCount(1, $deadLetters);
        self::assertTrue($deadLetters[0]->message->id->equals($message->id));
        self::assertSame(1, $deadLetters[0]->message->attempts);
        self::assertSame(FailureKind::HandlerFailed, $deadLetters[0]->reason->kind);
        self::assertSame('handler exploded', $deadLetters[0]->reason->errorMessage);
        self::assertSame(RuntimeException::class, $deadLetters[0]->reason->errorClass);

        self::assertSame(1, $transport->requeueDeadLetters());
        self::assertSame([], $transport->deadLetters());

        [$requeued] = $this->receive($transport, 1);
        self::assertTrue($requeued->message->id->equals($message->id));
        self::assertSame(0, $requeued->message->attempts);
    }

    final public function testRequeueSingleDeadLetterAndPurge(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof DeadLetterAwareInterface) {
            self::markTestSkipped('Transport without dead letter queue.');
        }

        $keep = $this->message('test', 'keep');
        $requeue = $this->message('test', 'requeue');
        foreach ([$keep, $requeue] as $message) {
            $transport->send($message);
            [$delivery] = $this->receive($transport, 1);
            $transport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->clock()->now()));
        }

        self::assertTrue($transport->requeueDeadLetter($requeue->id));
        self::assertFalse($transport->requeueDeadLetter($requeue->id));
        self::assertSame(['requeue'], $this->texts($this->drain($transport)));

        self::assertCount(1, $transport->deadLetters(1));
        self::assertSame(1, $transport->purgeDeadLetters());
        self::assertSame([], $transport->deadLetters());
        self::assertSame(0, $transport->purgeDeadLetters());
    }

    final public function testReleaseReturnsTheMessageWithoutCountingAnAttempt(): void
    {
        $transport = $this->transport();
        $transport->send($this->message('test', 'first'));
        $transport->send($this->message('test', 'second'));

        [$first] = $this->receive($transport, 1);
        $transport->release($first);

        $deliveries = $this->drain($transport);
        self::assertContains('first', $this->texts($deliveries));
        self::assertCount(2, $deliveries);
        foreach ($deliveries as $delivery) {
            self::assertSame(0, $delivery->message->attempts);
        }
    }

    final public function testConcurrentConsumersNeverReceiveTheSameMessage(): void
    {
        $transport = $this->transport();
        $consumer = $this->createConsumer($transport);
        for ($i = 0; $i < 20; ++$i) {
            $transport->send($this->message('test', 'm'.$i));
        }

        $seen = [];
        do {
            $batch = [...$this->receive($transport, 3), ...$this->receive($consumer, 3)];
            foreach ($batch as $delivery) {
                $seen[] = $delivery->message->text;
            }
        } while ([] !== $batch);

        sort($seen);
        $expected = array_map(static fn (int $i): string => 'm'.$i, range(0, 19));
        sort($expected);
        self::assertSame($expected, $seen);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function payloads(): iterable
    {
        yield 'valid json' => ['{"user":{"id":42,"tags":["a","b"]}}'];
        yield 'invalid json' => ['{"user":'];
        yield 'free text' => ['some json, some data, some text'];
        yield 'empty' => [''];
        yield 'utf-8' => ['è ✓ 😀 日本語'];
        yield 'newlines and nul-free control characters' => ["line1\nline2\r\n\tend"];
        yield 'large (1 MiB)' => [str_repeat('x', 1_048_576)];
    }

    #[DataProvider('payloads')]
    final public function testPayloadIsDeliveredByteForByte(string $text): void
    {
        $transport = $this->transport();
        $message = $this->message('test', $text, ['h' => 'è']);

        $transport->send($message);
        [$delivery] = $this->receive($transport, 1);

        $this->assertSameMessage($message, $delivery->message);
    }

    final public function testLargeBatch(): void
    {
        $transport = $this->transport();
        $transport->sendBatch((function (): iterable {
            for ($i = 0; $i < 500; ++$i) {
                yield new Envelope($this->message('test', 'm'.$i));
            }
        })());

        self::assertCount(500, $this->drain($transport));
    }

    final public function testStats(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof StatsAwareInterface) {
            self::markTestSkipped('Transport without stats.');
        }

        $transport->send($this->message('test', 'ready'));
        $transport->send($this->message('test', 'in flight'));
        $transport->send($this->message('test', 'delayed'), 60_000);
        $this->receive($transport, 1);

        $queueStats = $transport->stats();

        self::assertSame(1, $queueStats->ready);
        self::assertSame(1, $queueStats->inFlight);
        self::assertSame(1, $queueStats->delayed);
        self::assertSame(0, $queueStats->deadLetters);
        if ($transport->capabilities()->consumerLag) {
            self::assertSame(1, $queueStats->lag);
        }
    }

    final public function testBrowseListsReadyMessagesWithoutConsumingThem(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof BrowseAwareInterface) {
            self::markTestSkipped('Transport without browse.');
        }

        self::assertSame([], $transport->browse());

        $first = $this->message('test', 'first');
        $transport->send($first);
        $transport->send($this->message('test', 'second'));
        $transport->send($this->message('test', 'third'));

        $queued = $transport->browse();
        self::assertSame(['first', 'second', 'third'], array_map(static fn (QueuedMessage $queuedMessage): string => $queuedMessage->message->text, $queued));
        self::assertTrue($queued[0]->message->id->equals($first->id));
        foreach ($queued as $queuedMessage) {
            self::assertSame(QueuedMessageState::Ready, $queuedMessage->state);
            self::assertNull($queuedMessage->availableAt);
            self::assertSame(0, $queuedMessage->message->attempts);
        }

        self::assertCount(2, $transport->browse(2));

        $received = $this->drain($transport);
        // Nessun messaggio consumato; l'ordine non è garantito (RabbitMQ rimette in fondo quelli letti da browse(2)).
        self::assertEqualsCanonicalizing(['first', 'second', 'third'], $this->texts($received), 'browse() does not consume');
        foreach ($received as $delivery) {
            self::assertSame(0, $delivery->message->attempts);
        }
    }

    final public function testBrowseShowsInFlightAndDelayedMessages(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof BrowseAwareInterface || !$this->browsesInFlightAndDelayed()) {
            self::markTestSkipped('Transport that lists ready messages only.');
        }

        $transport->send($this->message('test', 'in flight'));
        $this->receive($transport, 1);
        $transport->send($this->message('test', 'later'), 60_000);
        $transport->send($this->message('test', 'sooner'), 30_000);
        $transport->send($this->message('test', 'ready'));

        $expectedSoonerMs = (int) $this->clock()->now()->format('Uv') + 30_000;

        $queued = $transport->browse();

        self::assertSame(['in flight', 'ready', 'sooner', 'later'], array_map(static fn (QueuedMessage $queuedMessage): string => $queuedMessage->message->text, $queued));
        self::assertSame(
            [QueuedMessageState::InFlight, QueuedMessageState::Ready, QueuedMessageState::Delayed, QueuedMessageState::Delayed],
            array_map(static fn (QueuedMessage $queuedMessage): QueuedMessageState => $queuedMessage->state, $queued),
        );
        self::assertEqualsWithDelta($expectedSoonerMs, (int) $queued[2]->availableAt?->format('Uv'), 1000);
        self::assertCount(3, $transport->browse(3));
    }

    final public function testBrowseRejectsNonPositiveLimit(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof BrowseAwareInterface) {
            self::markTestSkipped('Transport without browse.');
        }

        $this->expectException(InvalidArgumentException::class);

        $transport->browse(0);
    }

    final public function testPurgeRemovesReadyAndDelayedButKeepsInFlightAndDeadLetters(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof PurgeAwareInterface) {
            self::markTestSkipped('Transport without purge.');
        }

        self::assertSame(0, $transport->purge());

        $dead = $this->message('test', 'dead');
        $transport->send($dead);
        [$deadDelivery] = $this->receive($transport, 1);
        $transport->reject($deadDelivery, new FailureReason(FailureKind::Unrecoverable, $dead->id, $this->clock()->now()));

        $transport->send($this->message('test', 'in flight'));
        [$inFlight] = $this->receive($transport, 1);
        self::assertSame('in flight', $inFlight->message->text);

        $transport->send($this->message('test', 'ready 1'));
        $transport->send($this->message('test', 'ready 2'));
        $transport->send($this->message('test', 'delayed'), 2000);

        self::assertSame(3, $transport->purge());
        self::assertSame(0, $transport->purge());

        // Il messaggio in elaborazione segue il suo corso: se il consumer crasha torna in coda.
        $this->crash($transport);
        self::assertSame(['in flight'], $this->texts($this->drain($transport)));

        $this->maintain($transport, 2500);
        self::assertSame([], $this->drain($transport), 'the delayed message was purged too');

        if ($transport instanceof DeadLetterAwareInterface) {
            self::assertCount(1, $transport->deadLetters());
        }

        $transport->send($this->message('test', 'after purge'));
        self::assertSame(['after purge'], $this->texts($this->drain($transport)), 'the queue is still usable');
    }

    final public function testPurgeOfABacklogLargerThanOnePage(): void
    {
        $transport = $this->transport();
        if (!$transport instanceof PurgeAwareInterface) {
            self::markTestSkipped('Transport without purge.');
        }

        $transport->send($this->message('test', 'in flight'));
        [$inFlight] = $this->receive($transport, 1);
        $transport->sendBatch((function (): iterable {
            for ($i = 0; $i < 1200; ++$i) {
                yield new Envelope($this->message('test', 'm'.$i));
            }
        })());

        self::assertSame(1200, $transport->purge());

        $transport->ack($inFlight);
        self::assertSame([], $this->drain($transport));
    }

    final public function testSetupIsIdempotentAndPingSucceeds(): void
    {
        $transport = $this->transport();
        if ($transport instanceof SetupAwareInterface) {
            $transport->setup();
            $transport->setup();
        }

        $transport->ping();
        $transport->send($this->message('test', 'still works'));

        self::assertCount(1, $this->receive($transport, 1));
    }

    final public function testReceiveRejectsNonPositiveMax(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->transport()->receive(0);
    }

    final public function testReceiveRejectsNegativeTimeout(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->transport()->receive(1, -1);
    }

    final public function testSendRejectsNegativeDelay(): void
    {
        $transport = $this->transport();

        $this->expectException(InvalidArgumentException::class);

        $transport->send($this->message('test', 'x'), -1);
    }

    /**
     * Transport su una coda nuova e vuota, già pronto (setup eseguito).
     */
    abstract protected function createTransport(QueueConfig $queueConfig): TransportInterface;

    /**
     * Un secondo consumer della stessa coda di $transport.
     */
    abstract protected function createConsumer(TransportInterface $transport): TransportInterface;

    /**
     * Fa trascorrere il tempo (orologio finto o attesa reale).
     */
    abstract protected function advanceTimeMs(int $milliseconds): void;

    abstract protected function clock(): ClockInterface;

    /**
     * false per i backend che elencano solo i messaggi pronti (RabbitMQ).
     */
    protected function browsesInFlightAndDelayed(): bool
    {
        return true;
    }

    protected function queueConfig(): QueueConfig
    {
        return new QueueConfig(name: 'contract', claimIdleMs: self::CLAIM_IDLE_MS);
    }

    /**
     * @param array<string, string> $headers
     */
    final protected function message(string $channel, string $text, array $headers = []): Message
    {
        $this->uuidV7Generator ??= new UuidV7Generator($this->clock());
        $now = $this->clock()->now();

        return new Message($this->uuidV7Generator->generate(), $channel, $text, $now, 0, $now->modify('+1 day'), $headers);
    }

    /**
     * @return list<Delivery>
     */
    final protected function receive(TransportInterface $transport, int $max): array
    {
        return $transport->receive($max, 200);
    }

    /**
     * Riceve e conferma tutto ciò che è disponibile.
     *
     * @return list<Delivery>
     */
    final protected function drain(TransportInterface $transport): array
    {
        $all = [];
        while ([] !== ($batch = $this->receive($transport, 100))) {
            foreach ($batch as $delivery) {
                $transport->ack($delivery);
                $all[] = $delivery;
            }
        }

        return $all;
    }

    /**
     * Il consumer si ferma senza confermare: le sue consegne diventano orfane e vengono recuperate.
     * Di default trascorre claimIdleMs e si esegue la manutenzione; i backend con recupero nativo
     * (RabbitMQ) chiudono invece il canale.
     */
    protected function crash(TransportInterface $transport): void
    {
        $this->maintain($transport, self::CLAIM_IDLE_MS);
    }

    /**
     * Fa trascorrere $milliseconds e poi esegue la manutenzione, se il transport la richiede.
     */
    final protected function maintain(TransportInterface $transport, int $milliseconds): void
    {
        $this->advanceTimeMs($milliseconds);
        if ($transport instanceof MaintenanceAwareInterface) {
            $transport->maintain();
        }
    }

    /**
     * @param list<Delivery> $deliveries
     *
     * @return list<string>
     */
    final protected function texts(array $deliveries): array
    {
        return array_map(static fn (Delivery $delivery): string => $delivery->message->text, $deliveries);
    }

    private function transport(): TransportInterface
    {
        return $this->createTransport($this->queueConfig());
    }

    private function assertSameMessage(Message $expected, Message $actual): void
    {
        self::assertTrue($expected->id->equals($actual->id));
        self::assertSame($expected->channel, $actual->channel);
        self::assertSame($expected->text, $actual->text);
        self::assertSame($expected->attempts, $actual->attempts);
        self::assertSame($expected->headers, $actual->headers);
        self::assertSame($expected->createdAt->format('Uv'), $actual->createdAt->format('Uv'));
        self::assertSame($expected->expiresAt?->format('Uv'), $actual->expiresAt?->format('Uv'));
    }
}
