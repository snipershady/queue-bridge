<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis\Client;

use ArrayObject;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Predis\Client;
use Predis\ClientInterface;
use Predis\Command\CommandInterface;
use Predis\Connection\ConnectionException as PredisConnectionException;
use Predis\Connection\NodeConnectionInterface;
use Predis\Connection\RelayConnection;
use Predis\Response\Error;
use Predis\Response\ServerException;
use Predis\Response\Status;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Transport\Redis\Client\PredisClient;
use QueueBridge\Transport\Redis\Client\PredisPipeline;
use stdClass;

#[CoversClass(PredisClient::class)]
#[CoversClass(PredisPipeline::class)]
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

    /**
     * @return iterable<string, array{array<string, bool>}>
     */
    public static function exceptionOptions(): iterable
    {
        yield 'exceptions on (Predis default)' => [[]];
        yield 'exceptions off' => [['exceptions' => false]];
    }

    /**
     * @param array<string, bool> $options
     */
    #[DataProvider('exceptionOptions')]
    public function testPipelineReportsErrorsPerCommandWhateverTheExceptionsOption(array $options): void
    {
        $written = new ArrayObject();
        $client = $this->pipelinedClient($written, $options, ['1-0', new Error('WRONGTYPE Operation against a key holding the wrong kind of value'), null, 1]);

        $results = (new PredisClient($client))->pipeline([['XADD', 's', '*', 'a', '1'], ['XADD', 'z', '*', 'b', '2'], ['XADD', 's', '*', 'c', '3'], ['ZADD', 'z', '1', 'm']]);

        self::assertSame(['XADD', 'XADD', 'XADD', 'ZADD'], $written->getArrayCopy(), 'one round trip: every command is written before reading');
        self::assertSame('1-0', $results[0]);
        self::assertInstanceOf(CommandFailedException::class, $results[1]);
        self::assertStringContainsString('WRONGTYPE', $results[1]->getMessage());
        self::assertInstanceOf(CommandFailedException::class, $results[2]);
        self::assertSame('XADD failed: nil reply', $results[2]->getMessage());
        self::assertSame(1, $results[3], 'the commands after an error are reported as executed');
    }

    public function testPipelineIsRetriedOnceAfterAConnectionError(): void
    {
        $written = new ArrayObject();
        $connection = $this->createMock(NodeConnectionInterface::class);
        $connection->method('writeRequest')->willReturnCallback(static function (CommandInterface $command) use ($written): void {
            $written->append($command->getId());
        });
        $reads = 0;
        $connection->method('readResponse')->willReturnCallback(static function () use (&$reads): Status {
            if (1 === ++$reads) {
                throw new PredisConnectionException(self::createStub(NodeConnectionInterface::class), 'Connection reset');
            }

            return new Status('OK');
        });

        $results = (new PredisClient(new Client($connection)))->pipeline([['SET', 'k', 'v']]);

        self::assertSame(['OK'], $results);
        self::assertSame(['SET', 'SET'], $written->getArrayCopy());
    }

    public function testPipelineOverRelayRunsCommandsOneByOne(): void
    {
        $relayConnection = $this->createMock(RelayConnection::class);
        $relayConnection->expects(self::never())->method('writeRequest');
        $relayConnection->method('executeCommand')->willReturnOnConsecutiveCalls('1-0', new Error('OOM'));

        $results = (new PredisClient(new Client($relayConnection)))->pipeline([['XADD', 's', '*', 'a', '1'], ['XADD', 's', '*', 'b', '2']]);

        self::assertSame('1-0', $results[0]);
        self::assertInstanceOf(CommandFailedException::class, $results[1]);
    }

    /**
     * Client Predis reale su una connessione finta: la pipeline scrive tutti i comandi, poi legge $replies in ordine.
     *
     * @param ArrayObject<int, string> $written
     * @param array<string, bool>      $options
     * @param list<mixed>              $replies
     */
    private function pipelinedClient(ArrayObject $written, array $options, array $replies): Client
    {
        $connection = $this->createMock(NodeConnectionInterface::class);
        $connection->method('writeRequest')->willReturnCallback(static function (CommandInterface $command) use ($written): void {
            $written->append($command->getId());
        });
        $connection->method('readResponse')->willReturnOnConsecutiveCalls(...$replies);

        return new Client($connection, $options);
    }

    private function connectionError(): PredisConnectionException
    {
        return new PredisConnectionException(self::createStub(NodeConnectionInterface::class), 'Connection refused');
    }
}
