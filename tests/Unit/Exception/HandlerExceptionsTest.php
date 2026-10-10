<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Exception;

use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Exception\UnknownChannelException;
use snipershady\Queue\Exception\UnrecoverableMessageException;

#[CoversClass(RetryableException::class)]
#[CoversClass(UnrecoverableMessageException::class)]
#[CoversClass(UnknownChannelException::class)]
final class HandlerExceptionsTest extends TestCase
{
    public function testRetryableException(): void
    {
        $logicException = new LogicException('db down');

        $retryableException = new RetryableException('try later', 5000, $logicException);

        self::assertSame('try later', $retryableException->getMessage());
        self::assertSame(5000, $retryableException->delayMs);
        self::assertSame(0, $retryableException->getCode());
        self::assertSame($logicException, $retryableException->getPrevious());

        $defaults = new RetryableException();
        self::assertNull($defaults->delayMs);
        self::assertNull($defaults->getPrevious());
        self::assertSame(0, (new RetryableException(delayMs: 0))->delayMs);
    }

    public function testRetryableExceptionRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new RetryableException('x', -1);
    }

    public function testUnrecoverableMessageException(): void
    {
        $logicException = new LogicException('cause');

        $unrecoverableMessageException = new UnrecoverableMessageException('invalid order', $logicException);

        self::assertSame('invalid order', $unrecoverableMessageException->getMessage());
        self::assertSame(0, $unrecoverableMessageException->getCode());
        self::assertSame($logicException, $unrecoverableMessageException->getPrevious());
        self::assertNull((new UnrecoverableMessageException())->getPrevious());
    }

    public function testUnknownChannelException(): void
    {
        self::assertSame('No handler registered for channel "orders".', UnknownChannelException::forChannel('orders')->getMessage());
    }
}
