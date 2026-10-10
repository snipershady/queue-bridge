<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Exception;

use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Exception\QueueException;
use snipershady\Queue\Exception\SerializationException;

#[CoversClass(SerializationException::class)]
#[CoversClass(QueueException::class)]
final class SerializationExceptionTest extends TestCase
{
    public function testUnsupportedVersion(): void
    {
        $serializationException = SerializationException::unsupportedVersion('9');

        self::assertSame('Unsupported wire format version "9".', $serializationException->getMessage());
        self::assertNull($serializationException->getPrevious());
    }

    public function testInvalidFieldKeepsThePreviousException(): void
    {
        $logicException = new LogicException('cause');

        $serializationException = SerializationException::invalidField('id', 'bad.', $logicException);

        self::assertSame('Invalid wire format field "id": bad.', $serializationException->getMessage());
        self::assertSame(0, $serializationException->getCode());
        self::assertSame($logicException, $serializationException->getPrevious());
    }
}
