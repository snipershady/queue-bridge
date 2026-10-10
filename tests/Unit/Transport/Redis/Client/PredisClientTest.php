<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Transport\Redis\Client;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Predis\Client;
use Predis\ClientInterface;
use Predis\Command\CommandInterface;
use Predis\Connection\ConnectionException as PredisConnectionException;
use Predis\Connection\NodeConnectionInterface;
use Predis\Response\Error;
use Predis\Response\ServerException;
use Predis\Response\Status;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Transport\Redis\Client\PredisClient;
use stdClass;

#[CoversClass(PredisClient::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(ConnectionException::class)]
final class PredisClientTest extends TestCase
{
    public function testExecutesRawCommandsAndNormalizesReplies(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $sent = [];
        $client->method('executeCommand')->willReturnCallback(static function (CommandInterface $command) use (&$sent): mixed {
            $sent[] = [$command->getId(), ...$command->getArguments()];

            return match ($command->getId()) {
                'SET' => new Status('OK'),
                'GET' => null,
                'INCR' => 3,
                default => [new Status('QUEUED'), 'x', [null]],
            };
        });

        $predisClient = new PredisClient($client);

        self::assertSame('OK', $predisClient->execute(['SET', 'k', 'v']));
        self::assertNull($predisClient->execute(['GET', 'k']));
        self::assertSame(3, $predisClient->execute(['INCR', 'k']));
        self::assertSame(['QUEUED', 'x', [null]], $predisClient->execute(['EXEC']));
        self::assertSame(['SET', 'k', 'v'], $sent[0]);
    }

    public function testErrorResponsesBecomeCommandFailedException(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $client->method('executeCommand')->willReturn(new Error('WRONGTYPE bad'));

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('HSET failed: WRONGTYPE bad');

        (new PredisClient($client))->execute(['HSET', 'k', 'f', 'v']);
    }

    public function testServerExceptionsBecomeCommandFailedException(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $client->method('executeCommand')->willThrowException(new ServerException('NOSCRIPT No matching script'));

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('EVALSHA failed: NOSCRIPT');

        (new PredisClient($client))->execute(['EVALSHA', 'abc', '0']);
    }

    public function testRejectsUnknownReplyTypes(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $client->method('executeCommand')->willReturn(new stdClass());

        $this->expectException(CommandFailedException::class);

        (new PredisClient($client))->execute(['GET', 'k']);
    }

    public function testRetriesOnceAfterAConnectionError(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $client->expects(self::exactly(2))->method('executeCommand')->willReturnOnConsecutiveCalls(
            self::throwException($this->connectionError()),
            new Status('PONG'),
        );

        self::assertSame('PONG', (new PredisClient($client))->execute(['PING']));
    }

    public function testRepeatedConnectionErrorsBecomeConnectionException(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $client->method('executeCommand')->willThrowException($this->connectionError());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Redis backend is unreachable');

        (new PredisClient($client))->execute(['PING']);
    }

    public function testPipelineWithAGenericClientRunsCommandsOneByOne(): void
    {
        $client = $this->createMock(ClientInterface::class);
        $client->method('executeCommand')->willReturnOnConsecutiveCalls('1-0', new Error('OOM'), null);

        $results = (new PredisClient($client))->pipeline([['XADD', 's', '*', 'a', '1'], ['XADD', 's', '*', 'b', '2'], ['ZADD', 'z', '1', 'm']]);

        self::assertSame('1-0', $results[0]);
        self::assertInstanceOf(CommandFailedException::class, $results[1]);
        self::assertSame('XADD failed: OOM', $results[1]->getMessage());
        self::assertInstanceOf(CommandFailedException::class, $results[2]);
        self::assertSame('ZADD failed: nil reply', $results[2]->getMessage());
    }

    public function testNativePipelineReportsErrorsPerCommand(): void
    {
        $client = $this->createMock(Client::class);
        $queued = [];
        $client->method('pipeline')->willReturnCallback(static function (callable $callback) use (&$queued): array {
            $pipeline = (new self('pipeline'))->createMock(ClientInterface::class);
            $pipeline->method('executeCommand')->willReturnCallback(static function (CommandInterface $command) use (&$queued): null {
                $queued[] = $command->getId();

                return null;
            });
            $callback($pipeline);

            return ['1-0', new Error('OOM'), null, 1];
        });

        $results = (new PredisClient($client))->pipeline([['XADD', 's', '*', 'a', '1'], ['XADD', 's', '*', 'b', '2'], ['XADD', 's', '*', 'c', '3'], ['ZADD', 'z', '1', 'm']]);

        self::assertSame(['XADD', 'XADD', 'XADD', 'ZADD'], $queued);
        self::assertSame('1-0', $results[0]);
        self::assertInstanceOf(CommandFailedException::class, $results[1]);
        self::assertInstanceOf(CommandFailedException::class, $results[2]);
        self::assertSame('XADD failed: nil reply', $results[2]->getMessage());
        self::assertSame(1, $results[3]);
    }

    public function testNativePipelineWithExceptionsMarksEveryCommandAsFailed(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('pipeline')->willThrowException(new ServerException('WRONGTYPE'));

        $results = (new PredisClient($client))->pipeline([['XADD', 's', '*', 'a', '1'], ['ZADD', 'z', '1', 'm']]);

        self::assertCount(2, $results);
        foreach ($results as $result) {
            self::assertInstanceOf(CommandFailedException::class, $result);
            self::assertStringContainsString('WRONGTYPE', $result->getMessage());
        }
    }

    public function testNativePipelineWithoutRepliesMarksEveryCommandAsFailed(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('pipeline')->willReturn(null);

        $results = (new PredisClient($client))->pipeline([['XADD', 's', '*', 'a', '1']]);

        self::assertInstanceOf(CommandFailedException::class, $results[0]);
    }

    private function connectionError(): PredisConnectionException
    {
        return new PredisConnectionException(self::createStub(NodeConnectionInterface::class), 'Connection refused');
    }
}
