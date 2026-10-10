<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Factory;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Factory\QueueFactory;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Factory\TransportBuilderInterface;
use snipershady\Queue\Message\Envelope;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Message\OutgoingMessage;
use snipershady\Queue\Producer\Producer;
use snipershady\Queue\Producer\ProducerMiddlewareInterface;
use snipershady\Queue\Queue;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\System\UuidV7Generator;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\RecordingLogger;
use snipershady\Queue\Tests\Support\RecordingTransport;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;

#[CoversClass(QueueFactory::class)]
#[CoversClass(UnsupportedStrategyException::class)]
#[UsesClass(Queue::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(NullSignalHandler::class)]
#[UsesClass(NativeMemoryMeter::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(OutgoingMessage::class)]
#[UsesClass(Producer::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(UuidV7Generator::class)]
final class QueueFactoryTest extends TestCase
{
    public function testTheFirstBuilderSupportingTheDtoWins(): void
    {
        $recordingTransport = new RecordingTransport();
        $queueConfig = new QueueConfig(name: 'orders');
        $recordingLogger = new RecordingLogger();
        $queueFactory = new QueueFactory(
            [$this->builder('other', new RecordingTransport()), $this->builder('custom', $recordingTransport), $this->builder('custom', new RecordingTransport())],
            new FakeClock(),
            $recordingLogger,
        );

        $queue = $queueFactory->create($this->dto('custom'), $queueConfig);

        self::assertSame($recordingTransport, $queue->transport());
        self::assertSame(['debug'], $recordingLogger->levels());
        self::assertSame(['queue' => 'orders', 'strategy' => 'custom'], $recordingLogger->records[0]['context']);
    }

    public function testCustomBackendsPlugInWithoutTouchingTheCore(): void
    {
        $queueFactory = new QueueFactory([$this->builder('sqs', new RecordingTransport())]);

        $queue = $queueFactory->create($this->dto('sqs'));

        self::assertSame(OrphanRecovery::Native, $queue->capabilities()->orphanRecovery);
    }

    public function testProducerMiddlewaresReachTheQueue(): void
    {
        $recordingTransport = new RecordingTransport();
        $middleware = new class implements ProducerMiddlewareInterface {
            #[Override]
            public function process(Message $message, callable $next): void
            {
                $next($message->withHeaders(['source' => 'factory']));
            }
        };

        (new QueueFactory([$this->builder('custom', $recordingTransport)], new FakeClock(), producerMiddlewares: [$middleware]))
            ->create($this->dto('custom'))
            ->producer()
            ->publish('test', 'x');

        self::assertSame(['source' => 'factory'], $recordingTransport->sent[0]->message->headers);
    }

    public function testUnsupportedDtoListsTheRegisteredBuilders(): void
    {
        $builder = $this->builder('redis', new RecordingTransport());

        $this->expectException(UnsupportedStrategyException::class);
        $this->expectExceptionMessageMatches('/^No transport builder supports the "sqs" strategy \(.+\)\. Registered builders: .+\.$/');

        (new QueueFactory([$builder]))->create($this->dto('sqs'));
    }

    public function testUnsupportedStrategyMessageWithoutBuilders(): void
    {
        self::assertStringEndsWith('Registered builders: none.', UnsupportedStrategyException::noBuilderFor($this->dto('x'), [])->getMessage());
    }

    public function testRequiresAtLeastOneBuilder(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new QueueFactory([]);
    }

    private function dto(string $name): StrategyDtoInterface
    {
        return new readonly class($name) implements StrategyDtoInterface {
            public function __construct(public string $backend)
            {
            }

            #[Override]
            public function name(): string
            {
                return $this->backend;
            }
        };
    }

    private function builder(string $accepts, TransportInterface $transport): TransportBuilderInterface
    {
        return new readonly class($accepts, $transport) implements TransportBuilderInterface {
            public function __construct(private string $accepts, private TransportInterface $transport)
            {
            }

            #[Override]
            public function supports(StrategyDtoInterface $strategy): bool
            {
                return $strategy->name() === $this->accepts;
            }

            #[Override]
            public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
            {
                return $this->transport;
            }
        };
    }
}
