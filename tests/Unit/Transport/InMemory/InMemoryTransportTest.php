<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\InMemory;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use Psr\Clock\ClockInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Contract\TransportContractTestCase;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Transport\DeadLetter;
use QueueBridge\Transport\InMemory\InMemoryBroker;
use QueueBridge\Transport\InMemory\InMemoryEntry;
use QueueBridge\Transport\InMemory\InMemoryTransport;
use QueueBridge\Transport\MaintenanceReport;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;

#[CoversClass(InMemoryTransport::class)]
#[CoversClass(InMemoryBroker::class)]
#[CoversClass(InMemoryEntry::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(FailureReason::class)]
#[UsesClass(DeadLetter::class)]
#[UsesClass(MaintenanceReport::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(QueuedMessage::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(UuidV7Generator::class)]
#[UsesClass(NativeRandom::class)]
final class InMemoryTransportTest extends TransportContractTestCase
{
    private FakeClock $fakeClock;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock();
    }

    public function testCapabilities(): void
    {
        $transportCapabilities = $this->inMemory()->capabilities();

        self::assertSame(OrphanRecovery::Explicit, $transportCapabilities->orphanRecovery);
        self::assertTrue($transportCapabilities->nativeDelay);
        self::assertTrue($transportCapabilities->batchReceive);
        self::assertTrue($transportCapabilities->batchAck);
        self::assertTrue($transportCapabilities->deadLetter);
        self::assertTrue($transportCapabilities->consumerLag);
        self::assertTrue($transportCapabilities->deliveryCount);
    }

    public function testDelayedMessagesBecomeAvailableWithoutMaintenance(): void
    {
        $inMemoryTransport = $this->inMemory();
        $inMemoryTransport->send($this->message('test', 'b'), 2000);
        $inMemoryTransport->send($this->message('test', 'a'), 1000);
        $inMemoryTransport->send($this->message('test', 'c'), 2000);

        $this->fakeClock->advanceMs(2000);

        self::assertSame(['a', 'b', 'c'], $this->texts($inMemoryTransport->receive(10)));
    }

    public function testMaintenanceReportsPromotedAndRecovered(): void
    {
        $inMemoryTransport = $this->inMemory();
        $inMemoryTransport->send($this->message('test', 'delayed'), 500);
        $inMemoryTransport->send($this->message('test', 'orphan'));
        $inMemoryTransport->receive();

        $this->fakeClock->advanceMs(self::CLAIM_IDLE_MS);
        $maintenanceReport = $inMemoryTransport->maintain();

        self::assertSame(1, $maintenanceReport->promotedDelayed);
        self::assertSame(1, $maintenanceReport->recoveredOrphans);
        self::assertSame(0, $maintenanceReport->trimmed);
        $idle = $inMemoryTransport->maintain();
        self::assertSame([0, 0, 0], [$idle->promotedDelayed, $idle->recoveredOrphans, $idle->trimmed]);
    }

    public function testDeadLetterQueueKeepsOnlyTheNewestMessagesWithinRetention(): void
    {
        $inMemoryTransport = new InMemoryTransport($this->fakeClock, new QueueConfig(dlqMaxLength: 2));
        foreach (['old', 'middle', 'new'] as $text) {
            $message = $this->message('test', $text);
            $inMemoryTransport->send($message);
            [$delivery] = $inMemoryTransport->receive();
            $inMemoryTransport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->fakeClock->now()));
        }

        self::assertSame(['middle', 'new'], array_map(static fn (DeadLetter $deadLetter): string => $deadLetter->message->text, $inMemoryTransport->deadLetters()));
    }

    public function testRequeueDeadLettersHonoursLimit(): void
    {
        $inMemoryTransport = $this->inMemory();
        foreach (['a', 'b', 'c'] as $text) {
            $message = $this->message('test', $text);
            $inMemoryTransport->send($message);
            [$delivery] = $inMemoryTransport->receive();
            $inMemoryTransport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->fakeClock->now()));
        }

        self::assertSame(2, $inMemoryTransport->requeueDeadLetters(2));
        self::assertSame(['a', 'b'], $this->texts($inMemoryTransport->receive(10)));
        self::assertCount(1, $inMemoryTransport->deadLetters());
    }

    public function testRetryAndRejectOfUnknownDeliveryHaveNoEffect(): void
    {
        $inMemoryTransport = $this->inMemory();
        $message = $this->message('test', 'x');
        $delivery = new Delivery($message, 'mem-unknown', $this->fakeClock->now());

        $inMemoryTransport->retry($delivery, 0);
        $inMemoryTransport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->fakeClock->now()));
        $inMemoryTransport->release($delivery);

        $queueStats = $inMemoryTransport->stats();
        self::assertSame([0, 0, 0, 0, 0], [$queueStats->ready, $queueStats->inFlight, $queueStats->delayed, $queueStats->deadLetters, $queueStats->lag]);
    }

    public function testDeadLettersHonoursLimitAndSingleRequeueRemovesOnlyThatMessage(): void
    {
        $inMemoryTransport = $this->inMemory();
        $messages = [];
        foreach (['a', 'b', 'c'] as $text) {
            $message = $this->message('test', $text);
            $messages[] = $message;
            $inMemoryTransport->send($message);
            [$delivery] = $inMemoryTransport->receive();
            $inMemoryTransport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $message->id, $this->fakeClock->now()));
        }

        self::assertCount(2, $inMemoryTransport->deadLetters(2));
        self::assertTrue($inMemoryTransport->requeueDeadLetter($messages[1]->id));
        self::assertSame(['a', 'c'], array_map(static fn (DeadLetter $deadLetter): string => $deadLetter->message->text, $inMemoryTransport->deadLetters()));
    }

    public function testStatsCountsDueDelayedMessagesAsReady(): void
    {
        $inMemoryTransport = $this->inMemory();
        $inMemoryTransport->send($this->message('test', 'delayed'), 1000);

        $this->fakeClock->advanceMs(1000);
        $queueStats = $inMemoryTransport->stats();

        self::assertSame(1, $queueStats->ready);
        self::assertSame(0, $queueStats->delayed);
    }

    public function testStatsLagCountsOnlyNeverDeliveredMessages(): void
    {
        $inMemoryTransport = $this->inMemory();
        $inMemoryTransport->send($this->message('test', 'released'));
        $inMemoryTransport->send($this->message('test', 'fresh'));
        [$delivery] = $inMemoryTransport->receive();
        $inMemoryTransport->release($delivery);

        $queueStats = $inMemoryTransport->stats();

        self::assertSame(2, $queueStats->ready);
        self::assertSame(1, $queueStats->lag);
        self::assertNull($queueStats->consumers);
    }

    public function testDeadLettersRejectsNonPositiveLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->inMemory()->deadLetters(0);
    }

    public function testRequeueDeadLettersRejectsNonPositiveLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->inMemory()->requeueDeadLetters(0);
    }

    public function testRetryRejectsNegativeDelay(): void
    {
        $inMemoryTransport = $this->inMemory();
        $inMemoryTransport->send($this->message('test', 'x'));
        [$delivery] = $inMemoryTransport->receive();

        $this->expectException(InvalidArgumentException::class);

        $inMemoryTransport->retry($delivery, -1);
    }

    #[Override]
    protected function createTransport(QueueConfig $queueConfig): TransportInterface
    {
        return new InMemoryTransport($this->fakeClock, $queueConfig);
    }

    #[Override]
    protected function createConsumer(TransportInterface $transport): TransportInterface
    {
        self::assertInstanceOf(InMemoryTransport::class, $transport);

        return $transport->newConsumer();
    }

    #[Override]
    protected function advanceTimeMs(int $milliseconds): void
    {
        $this->fakeClock->advanceMs($milliseconds);
    }

    #[Override]
    protected function clock(): ClockInterface
    {
        return $this->fakeClock;
    }

    private function inMemory(): InMemoryTransport
    {
        return new InMemoryTransport($this->fakeClock, $this->queueConfig());
    }
}
