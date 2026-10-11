<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Factory;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\UnsupportedStrategyException;
use QueueBridge\Factory\QueueFactory;
use QueueBridge\Factory\StrategyDtoInterface;
use QueueBridge\Factory\TransportBuilderInterface;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Message\OutgoingMessage;
use QueueBridge\Producer\Producer;
use QueueBridge\Producer\ProducerMiddlewareInterface;
use QueueBridge\Queue;
use QueueBridge\System\NativeMemoryMeter;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\NullSignalHandler;
use QueueBridge\System\SystemClock;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Tests\Support\RecordingTransport;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;

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
