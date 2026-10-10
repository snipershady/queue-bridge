<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Exception;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use RuntimeException;
use snipershady\Queue\Exception\QueueException;

#[CoversClass(QueueException::class)]
final class QueueExceptionTest extends TestCase
{
    public function testIsAnAbstractRuntimeException(): void
    {
        $reflectionClass = new ReflectionClass(QueueException::class);

        self::assertTrue($reflectionClass->isAbstract());
        self::assertTrue($reflectionClass->isSubclassOf(RuntimeException::class));
    }

    public function testSubclassCarriesMessageCodeAndPrevious(): void
    {
        $runtimeException = new RuntimeException('cause');
        $queueException = new class('failure', 42, $runtimeException) extends QueueException {};

        self::assertSame('failure', $queueException->getMessage());
        self::assertSame(42, $queueException->getCode());
        self::assertSame($runtimeException, $queueException->getPrevious());
    }
}
