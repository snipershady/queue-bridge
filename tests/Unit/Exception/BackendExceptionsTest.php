<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Exception;

use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Exception\BackendException;
use QueueBridge\Exception\BatchSendException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Exception\QueueException;
use QueueBridge\Message\MessageId;
use ReflectionClass;

#[CoversClass(BackendException::class)]
#[CoversClass(ConnectionException::class)]
#[CoversClass(BatchSendException::class)]
#[UsesClass(MessageId::class)]
final class BackendExceptionsTest extends TestCase
{
    public function testBackendExceptionIsAnAbstractQueueException(): void
    {
        $reflectionClass = new ReflectionClass(BackendException::class);

        self::assertTrue($reflectionClass->isAbstract());
        self::assertTrue($reflectionClass->isSubclassOf(QueueException::class));
    }

    public function testConnectionException(): void
    {
        $logicException = new LogicException('refused');

        $connectionException = ConnectionException::unreachable('redis', 'connection refused', $logicException);

        self::assertSame('redis backend is unreachable: connection refused', $connectionException->getMessage());
        self::assertSame(0, $connectionException->getCode());
        self::assertSame($logicException, $connectionException->getPrevious());
    }

    public function testBatchSendExceptionReportsSentAndFailedIds(): void
    {
        $sent = [MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd')];
        $failed = [
            MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abce'),
            MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcf'),
        ];
        $logicException = new LogicException('timeout');

        $batchSendException = new BatchSendException($sent, $failed, $logicException);

        self::assertSame($sent, $batchSendException->sentIds);
        self::assertSame($failed, $batchSendException->failedIds);
        self::assertSame('Batch send failed: 1 sent, 2 failed.', $batchSendException->getMessage());
        self::assertSame(0, $batchSendException->getCode());
        self::assertSame($logicException, $batchSendException->getPrevious());
        self::assertNull((new BatchSendException([], []))->getPrevious());
    }
}
