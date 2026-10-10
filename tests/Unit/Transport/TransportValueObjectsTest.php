<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Transport;

use DateTimeImmutable;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Transport\DeadLetter;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;

#[CoversClass(QueueStats::class)]
#[CoversClass(MaintenanceReport::class)]
#[CoversClass(TransportCapabilities::class)]
#[CoversClass(OrphanRecovery::class)]
#[CoversClass(DeadLetter::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(FailureReason::class)]
final class TransportValueObjectsTest extends TestCase
{
    public function testQueueStats(): void
    {
        $queueStats = new QueueStats(1, 2, 3, 4, 5, 6);

        self::assertSame([1, 2, 3, 4, 5, 6], [$queueStats->ready, $queueStats->inFlight, $queueStats->delayed, $queueStats->deadLetters, $queueStats->lag, $queueStats->consumers]);

        $withoutOptional = new QueueStats(0, 0, 0, 0);
        self::assertNull($withoutOptional->lag);
        self::assertNull($withoutOptional->consumers);
    }

    /**
     * @return iterable<string, array{array{int, int, int, int, ?int, ?int}, string}>
     */
    public static function negativeStats(): iterable
    {
        yield 'ready' => [[-1, 0, 0, 0, null, null], 'ready must be >= 0'];
        yield 'inFlight' => [[0, -1, 0, 0, null, null], 'inFlight must be >= 0'];
        yield 'delayed' => [[0, 0, -1, 0, null, null], 'delayed must be >= 0'];
        yield 'deadLetters' => [[0, 0, 0, -1, null, null], 'deadLetters must be >= 0'];
        yield 'lag' => [[0, 0, 0, 0, -1, null], 'lag must be >= 0'];
        yield 'consumers' => [[0, 0, 0, 0, null, -1], 'consumers must be >= 0'];
    }

    /**
     * @param array{int, int, int, int, ?int, ?int} $arguments
     */
    #[DataProvider('negativeStats')]
    public function testQueueStatsRejectsNegativeCounters(array $arguments, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        new QueueStats(...$arguments);
    }

    public function testMaintenanceReport(): void
    {
        $maintenanceReport = new MaintenanceReport(1, 2, 3);

        self::assertSame([1, 2, 3], [$maintenanceReport->promotedDelayed, $maintenanceReport->recoveredOrphans, $maintenanceReport->trimmed]);
        $defaults = new MaintenanceReport();
        self::assertSame([0, 0, 0], [$defaults->promotedDelayed, $defaults->recoveredOrphans, $defaults->trimmed]);
    }

    /**
     * @return iterable<string, array{array{int, int, int}, string}>
     */
    public static function negativeReports(): iterable
    {
        yield 'promotedDelayed' => [[-1, 0, 0], 'promotedDelayed must be >= 0'];
        yield 'recoveredOrphans' => [[0, -1, 0], 'recoveredOrphans must be >= 0'];
        yield 'trimmed' => [[0, 0, -1], 'trimmed must be >= 0'];
    }

    /**
     * @param array{int, int, int} $arguments
     */
    #[DataProvider('negativeReports')]
    public function testMaintenanceReportRejectsNegativeCounters(array $arguments, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        new MaintenanceReport(...$arguments);
    }

    public function testCapabilitiesDefaultToTheMinimum(): void
    {
        $transportCapabilities = new TransportCapabilities(OrphanRecovery::Native);

        self::assertSame(OrphanRecovery::Native, $transportCapabilities->orphanRecovery);
        self::assertFalse($transportCapabilities->nativeDelay);
        self::assertFalse($transportCapabilities->batchReceive);
        self::assertFalse($transportCapabilities->batchAck);
        self::assertFalse($transportCapabilities->deadLetter);
        self::assertFalse($transportCapabilities->consumerLag);
        self::assertFalse($transportCapabilities->deliveryCount);
        self::assertSame(['native', 'explicit'], array_map(static fn (OrphanRecovery $orphanRecovery): string => $orphanRecovery->value, OrphanRecovery::cases()));
    }

    public function testDeadLetter(): void
    {
        $messageId = MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd');
        $message = new Message($messageId, 'test', 'x', new DateTimeImmutable());
        $failureReason = new FailureReason(FailureKind::Expired, $messageId, new DateTimeImmutable());

        $deadLetter = new DeadLetter($message, $failureReason);

        self::assertSame($message, $deadLetter->message);
        self::assertSame($failureReason, $deadLetter->reason);
    }
}
