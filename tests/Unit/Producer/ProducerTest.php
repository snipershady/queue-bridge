<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Producer;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Message\OutgoingMessage;
use QueueBridge\Producer\Producer;
use QueueBridge\Producer\ProducerMiddlewareInterface;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\SystemClock;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Support\CallLog;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\FakeRandom;
use QueueBridge\Tests\Support\RecordingTransport;
use QueueBridge\Transport\InMemory\InMemoryBroker;
use QueueBridge\Transport\InMemory\InMemoryEntry;
use QueueBridge\Transport\InMemory\InMemoryTransport;
use QueueBridge\Transport\TransportCapabilities;

#[CoversClass(Producer::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(OutgoingMessage::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(UuidV7Generator::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(InMemoryEntry::class)]
#[UsesClass(TransportCapabilities::class)]
final class ProducerTest extends TestCase
{
    private FakeClock $fakeClock;

    private InMemoryTransport $inMemoryTransport;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock('2026-01-01T00:00:00.000Z');
        $this->inMemoryTransport = new InMemoryTransport($this->fakeClock);
    }

    public function testPublishCreatesTheMessageAndSendsIt(): void
    {
        $producer = $this->producer();

        $messageId = $producer->publish('test', '{"a":1}', ['correlation_id' => 'c-1']);

        [$delivery] = $this->inMemoryTransport->receive();
        $message = $delivery->message;
        self::assertTrue($message->id->equals($messageId));
        self::assertSame('test', $message->channel);
        self::assertSame('{"a":1}', $message->text);
        self::assertSame(['correlation_id' => 'c-1'], $message->headers);
        self::assertSame(0, $message->attempts);
        self::assertSame('2026-01-01T00:00:00.000+00:00', $message->createdAt->format('Y-m-d\TH:i:s.vP'));
        self::assertNull($message->expiresAt);
    }

    public function testPublishWithTtlSetsTheExpiration(): void
    {
        $this->producer()->publish('test', 'x', ttlMs: 1500);

        [$delivery] = $this->inMemoryTransport->receive();
        self::assertSame('2026-01-01T00:00:01.500+00:00', $delivery->message->expiresAt?->format('Y-m-d\TH:i:s.vP'));
    }

    public function testPublishWithDelay(): void
    {
        $this->producer()->publish('test', 'x', delayMs: 1000);

        self::assertSame([], $this->inMemoryTransport->receive());
        $this->fakeClock->advanceMs(1000);
        self::assertCount(1, $this->inMemoryTransport->receive());
    }

    public function testPublishUsesTheGivenIdGenerator(): void
    {
        $producer = new Producer(
            $this->inMemoryTransport,
            clock: $this->fakeClock,
            idGenerator: new UuidV7Generator($this->fakeClock, new FakeRandom(0x00)),
        );

        self::assertSame('019b76da-a800-7000-8000-000000000000', $producer->publish('test', 'x')->value);
    }

    public function testDefaultsUseTheSystemClock(): void
    {
        $recordingTransport = new RecordingTransport();

        (new Producer($recordingTransport))->publish('test', 'x');

        self::assertCount(1, $recordingTransport->sent);
        self::assertSame('UTC', $recordingTransport->sent[0]->message->createdAt->getTimezone()->getName());
    }

    public function testPublishBatchSendsOnceAndReturnsIdsInOrder(): void
    {
        $recordingTransport = new RecordingTransport();
        $producer = new Producer($recordingTransport, clock: $this->fakeClock);

        $ids = $producer->publishBatch([
            new OutgoingMessage('test', 'a'),
            new OutgoingMessage('test', 'b', delayMs: 500),
        ]);

        self::assertCount(1, $recordingTransport->batches);
        self::assertSame([], $recordingTransport->sent);
        [$first, $second] = $recordingTransport->batches[0];
        self::assertSame(['a', 0], [$first->message->text, $first->delayMs]);
        self::assertSame(['b', 500], [$second->message->text, $second->delayMs]);
        self::assertSame([$first->message->id, $second->message->id], $ids);
    }

    public function testEmptyBatchSendsNothing(): void
    {
        $recordingTransport = new RecordingTransport();

        self::assertSame([], (new Producer($recordingTransport, clock: $this->fakeClock))->publishBatch([]));
        self::assertSame([], $recordingTransport->batches);
    }

    public function testTextLimitIsMeasuredInBytes(): void
    {
        $producer = $this->producer(new QueueConfig(maxTextBytes: 4));

        $producer->publish('test', 'èè');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Text is 6 bytes, limit is 4.');

        $producer->publish('test', 'èèè');
    }

    public function testHeadersLimitCountsNamesAndValues(): void
    {
        $producer = $this->producer(new QueueConfig(maxHeadersBytes: 6));

        $producer->publish('test', 'x', ['ab' => 'cdef']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Headers are 7 bytes, limit is 6.');

        $producer->publish('test', 'x', ['ab' => 'cdefg']);
    }

    public function testInvalidMessageInABatchSendsNothing(): void
    {
        $recordingTransport = new RecordingTransport();
        $producer = new Producer($recordingTransport, new QueueConfig(maxTextBytes: 1), $this->fakeClock);

        try {
            $producer->publishBatch([new OutgoingMessage('test', 'a'), new OutgoingMessage('test', 'too long')]);
            self::fail('Expected InvalidArgumentException');
        } catch (InvalidArgumentException) {
            self::assertSame([], $recordingTransport->batches);
        }
    }

    public function testMiddlewaresRunInOrderAndTheirHeadersAreChecked(): void
    {
        $callLog = new CallLog();
        $producer = $this->producer(new QueueConfig(maxHeadersBytes: 4), [
            $this->headerMiddleware('a', '1', $callLog),
            $this->headerMiddleware('b', '2', $callLog),
        ]);

        $producer->publish('test', 'x');

        self::assertSame(['a', 'b'], $callLog->calls);
        [$delivery] = $this->inMemoryTransport->receive();
        self::assertSame(['a' => '1', 'b' => '2'], $delivery->message->headers);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Headers are 6 bytes, limit is 4.');

        $producer->publish('test', 'x', ['c' => '3']);
    }

    public function testMiddlewareCanDropAMessage(): void
    {
        $recordingTransport = new RecordingTransport();
        $drop = new class implements ProducerMiddlewareInterface {
            #[Override]
            public function process(Message $message, callable $next): void
            {
                if ('drop' !== $message->text) {
                    $next($message);
                }
            }
        };
        $producer = new Producer($recordingTransport, clock: $this->fakeClock, middlewares: [$drop]);

        $droppedId = $producer->publish('test', 'drop');
        $ids = $producer->publishBatch([new OutgoingMessage('test', 'keep'), new OutgoingMessage('test', 'drop')]);

        self::assertSame([], $recordingTransport->sent);
        self::assertCount(1, $ids);
        self::assertCount(1, $recordingTransport->batches[0]);
        self::assertSame('keep', $recordingTransport->batches[0][0]->message->text);
        self::assertFalse($droppedId->equals($ids[0]));
    }

    /**
     * @param list<ProducerMiddlewareInterface> $middlewares
     */
    private function producer(QueueConfig $queueConfig = new QueueConfig(), array $middlewares = []): Producer
    {
        return new Producer($this->inMemoryTransport, $queueConfig, $this->fakeClock, middlewares: $middlewares);
    }

    private function headerMiddleware(string $name, string $value, CallLog $callLog): ProducerMiddlewareInterface
    {
        return new readonly class($name, $value, $callLog) implements ProducerMiddlewareInterface {
            public function __construct(private string $name, private string $value, private CallLog $callLog)
            {
            }

            #[Override]
            public function process(Message $message, callable $next): void
            {
                $this->callLog->record($this->name);
                $next($message->withHeaders([...$message->headers, $this->name => $this->value]));
            }
        };
    }
}
