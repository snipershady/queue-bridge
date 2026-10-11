<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Cli;

use ArrayObject;
use DateTimeImmutable;
use InvalidArgumentException;
use LogicException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use snipershady\Queue\Cli\Application;
use snipershady\Queue\Cli\ConsoleLogger;
use snipershady\Queue\Cli\Input;
use snipershady\Queue\Cli\Output;
use snipershady\Queue\Cli\UsageException;
use snipershady\Queue\Codec\EnvelopeCodec;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Dsn\Dsn;
use snipershady\Queue\Dsn\DsnParser;
use snipershady\Queue\Dsn\StrategyDtoFactory;
use snipershady\Queue\Dsn\StrategyEnum;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Factory\QueueFactory;
use snipershady\Queue\Factory\QueueFactoryInterface;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Factory\TransportBuilderInterface;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Queue;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\Retry\ExponentialBackoff;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\PcntlSignalHandler;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\RecordingTransport;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Transport\Capability\SetupAwareInterface;
use snipershady\Queue\Transport\DeadLetter;
use snipershady\Queue\Transport\InMemory\InMemoryBroker;
use snipershady\Queue\Transport\InMemory\InMemoryEntry;
use snipershady\Queue\Transport\InMemory\InMemoryTransport;
use snipershady\Queue\Transport\InMemory\InMemoryTransportBuilder;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Transport\MaintenanceReport;
use snipershady\Queue\Transport\QueuedMessage;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\RabbitMq\RabbitMqTransportBuilder;
use snipershady\Queue\Transport\Redis\RedisTransportBuilder;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;
use snipershady\Queue\Worker\CallableHandler;
use snipershady\Queue\Worker\Event\MessageHandled;
use snipershady\Queue\Worker\Event\MessageReceived;
use snipershady\Queue\Worker\Event\WorkerStopped;
use snipershady\Queue\Worker\HandlerRegistry;
use snipershady\Queue\Worker\StopConditions;
use snipershady\Queue\Worker\Worker;
use snipershady\Queue\Worker\WorkerConfig;

#[CoversClass(Application::class)]
#[CoversClass(Input::class)]
#[CoversClass(Output::class)]
#[CoversClass(ConsoleLogger::class)]
#[UsesClass(UsageException::class)]
#[UsesClass(Dsn::class)]
#[UsesClass(DsnParser::class)]
#[UsesClass(StrategyDtoFactory::class)]
#[UsesClass(StrategyEnum::class)]
#[UsesClass(StrategyDtoInMemory::class)]
#[UsesClass(QueueFactory::class)]
#[UsesClass(Queue::class)]
#[UsesClass(QueueBridge::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(InMemoryTransport::class)]
#[UsesClass(InMemoryTransportBuilder::class)]
#[UsesClass(RedisTransportBuilder::class)]
#[UsesClass(RabbitMqTransportBuilder::class)]
#[UsesClass(InMemoryBroker::class)]
#[UsesClass(InMemoryEntry::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(QueuedMessage::class)]
#[UsesClass(MaintenanceReport::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(FailureReason::class)]
#[UsesClass(DeadLetter::class)]
#[UsesClass(Worker::class)]
#[UsesClass(WorkerConfig::class)]
#[UsesClass(StopConditions::class)]
#[UsesClass(HandlerRegistry::class)]
#[UsesClass(CallableHandler::class)]
#[UsesClass(TextDecoder::class)]
#[UsesClass(DecodedText::class)]
#[UsesClass(ExponentialBackoff::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(NativeMemoryMeter::class)]
#[UsesClass(NullSignalHandler::class)]
#[UsesClass(PcntlSignalHandler::class)]
#[UsesClass(SystemClock::class)]
#[UsesClass(EnvelopeCodec::class)]
#[UsesClass(ConnectionException::class)]
#[UsesClass(UnsupportedStrategyException::class)]
#[UsesClass(MessageHandled::class)]
#[UsesClass(MessageReceived::class)]
#[UsesClass(WorkerStopped::class)]
final class CliTest extends TestCase
{
    private const string ID = '0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    /** @var resource */
    private mixed $stdout;

    /** @var resource */
    private mixed $stderr;

    private FakeClock $fakeClock;

    private InMemoryTransport $inMemoryTransport;

    #[Override]
    protected function setUp(): void
    {
        $stdout = fopen('php://memory', 'w+');
        $stderr = fopen('php://memory', 'w+');
        self::assertIsResource($stdout);
        self::assertIsResource($stderr);
        $this->stdout = $stdout;
        $this->stderr = $stderr;
        $this->fakeClock = new FakeClock();
        $this->inMemoryTransport = new InMemoryTransport($this->fakeClock, new QueueConfig(name: 'orders'));
    }

    // --- Input -------------------------------------------------------------------------------------------------

    public function testInputParsesCommandOptionsFlagsAndVerbosity(): void
    {
        $input = Input::parse(['work', '--dsn=redis://h', '--queue', 'orders', '--stop-when-empty', '-v', '-vv', '-h', '--json']);

        self::assertSame('work', $input->command);
        self::assertSame('redis://h', $input->value('dsn'));
        self::assertSame('orders', $input->value('queue'));
        self::assertTrue($input->flag('stop-when-empty'));
        self::assertTrue($input->flag('help'));
        self::assertTrue($input->flag('json'));
        self::assertFalse($input->flag('force'));
        self::assertNull($input->value('stop-when-empty'));
        self::assertSame(2, $input->verbosity);
        self::assertSame(1, Input::parse(['-v'])->verbosity);
        self::assertSame(0, Input::parse([])->verbosity);
        self::assertNull(Input::parse([])->command);
    }

    public function testInputIntegers(): void
    {
        $input = Input::parse(['--a=5', '--b=0', '--c=x']);

        self::assertSame(5, $input->int('a', default: null));
        self::assertSame(0, $input->int('b', default: null, minimum: 0));
        self::assertSame(7, $input->int('missing', 7));
        self::assertNull($input->int('missing', default: null));
        self::assertSame('1=2', Input::parse(['--a=1=2'])->value('a'), 'only the first "=" separates the value');

        $malformed = Input::parse(['--x=x1', '--y=1x', "--z=1\n"]);
        foreach ([
            static fn (): ?int => $input->int('b', default: null),
            static fn (): ?int => $input->int('c', default: null),
            static fn (): ?int => $malformed->int('x', default: null),
            static fn (): ?int => $malformed->int('y', default: null),
            static fn (): ?int => $malformed->int('z', default: null),
        ] as $call) {
            try {
                $call();
                self::fail('Expected UsageException');
            } catch (UsageException $usageException) {
                self::assertStringContainsString('must be an integer', $usageException->getMessage());
            }
        }
    }

    /**
     * @return iterable<string, array{list<string>, string}>
     */
    public static function invalidInputs(): iterable
    {
        yield 'second command' => [['work', 'stats'], 'Unexpected argument "stats"'];
        yield 'empty option' => [['--'], 'Empty option name'];
        yield 'flag with value' => [['--json=1'], 'does not take a value'];
        yield 'missing value at the end' => [['--dsn'], 'requires a value'];
        yield 'missing value before an option' => [['--dsn', '--json'], 'requires a value'];
    }

    /**
     * @param list<string> $arguments
     */
    #[DataProvider('invalidInputs')]
    public function testInputRejectsMalformedArguments(array $arguments, string $error): void
    {
        $this->expectException(UsageException::class);
        $this->expectExceptionMessage($error);

        Input::parse($arguments);
    }

    // --- Output e logger -----------------------------------------------------------------------------------------

    public function testOutputRequiresStreams(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Output('stdout', $this->stderr); // @phpstan-ignore argument.type (verifica a runtime di un input che viola il tipo dichiarato)
    }

    public function testConsoleLoggerFiltersByVerbosityAndInterpolates(): void
    {
        foreach ([0, 1, 2] as $verbosity) {
            $consoleLogger = new ConsoleLogger($this->streams(), $verbosity);
            $consoleLogger->debug('debug {n}', ['n' => 1]);
            $consoleLogger->info('info');
            $consoleLogger->warning('warn {a} {b} {c} {d} {e} {f}', [
                'a' => 'x', 'b' => null, 'c' => new DateTimeImmutable('2026-01-01T00:00:00Z'),
                'd' => new LogicException('boom'), 'e' => [1], 'f' => MessageId::fromString(self::ID),
            ]);
            $consoleLogger->log(42, 'non string level');
        }

        self::assertSame(\implode(\PHP_EOL, [
            '[warning] warn x null 2026-01-01T00:00:00+00:00 LogicException [array] '.self::ID,
            '[info] info',
            '[warning] warn x null 2026-01-01T00:00:00+00:00 LogicException [array] '.self::ID,
            '[info] non string level',
            '[debug] debug 1',
            '[info] info',
            '[warning] warn x null 2026-01-01T00:00:00+00:00 LogicException [array] '.self::ID,
            '[info] non string level',
        ]).\PHP_EOL, $this->stderr());
    }

    public function testConsoleLoggerDefaultsToWarningsAndTreatsUnknownLevelsAsInfo(): void
    {
        $consoleLogger = new ConsoleLogger($this->streams());
        $consoleLogger->notice('hidden notice');
        $consoleLogger->log('custom', 'hidden custom');
        $consoleLogger->warning('shown');
        (new ConsoleLogger($this->streams(), 1))->log('custom', 'shown custom');

        self::assertSame('[warning] shown'.\PHP_EOL.'[custom] shown custom'.\PHP_EOL, $this->stderr());
    }

    // --- Application ---------------------------------------------------------------------------------------------

    public function testHelp(): void
    {
        self::assertSame(Application::SUCCESS, $this->application()->run(['--help']));
        self::assertStringStartsWith('Usage: queue-worker <command> [options]', $this->stdout());
        self::assertSame(Application::SUCCESS, $this->application()->run(['stats', '-h']));
    }

    public function testNoCommandShowsTheUsageAndFails(): void
    {
        self::assertSame(Application::USAGE, $this->application()->run([]));
        self::assertStringContainsString('Commands:', $this->stdout());
    }

    public function testPingAndStatsUseTheDsnFromTheEnvironment(): void
    {
        $application = $this->application(environment: ['QUEUE_DSN' => 'memory://']);

        self::assertSame(Application::SUCCESS, $application->run(['ping']));
        self::assertSame(Application::SUCCESS, $application->run(['stats']));

        self::assertSame(\implode(\PHP_EOL, ['OK', 'ready         0', 'in_flight     0', 'delayed       0', 'dead_letters  0', 'lag           0', 'consumers     n/a']).\PHP_EOL, $this->stdout());
    }

    public function testStatsAsJson(): void
    {
        $this->send('a');

        self::assertSame(Application::SUCCESS, $this->sharedApplication()->run(['stats', '--dsn=memory://', '--json']));
        self::assertSame('{"ready":1,"in_flight":0,"delayed":0,"dead_letters":0,"lag":1,"consumers":null}'.\PHP_EOL, $this->stdout());
    }

    public function testStatsOnABackendWithoutStatistics(): void
    {
        self::assertSame(Application::FAILURE, $this->sharedApplication(new RecordingTransport())->run(['stats', '--dsn=memory://']));
        self::assertStringContainsString('does not provide statistics', $this->stderr());
    }

    public function testDeadLetterCommands(): void
    {
        $this->deadLetter('first');
        $this->deadLetter('second', '0190f2a4-5b6c-7d8e-9f01-000000000002');
        $application = $this->sharedApplication();

        self::assertSame(Application::SUCCESS, $application->run(['dlq:list', '--dsn=memory://', '--limit=1']));
        self::assertSame(Application::SUCCESS, $application->run(['dlq:list', '--dsn=memory://', '--json']));
        self::assertSame(Application::SUCCESS, $application->run(['dlq:requeue', '--dsn=memory://', '--id='.self::ID]));
        self::assertSame(Application::FAILURE, $application->run(['dlq:requeue', '--dsn=memory://', '--id='.self::ID]));
        self::assertSame(Application::SUCCESS, $application->run(['dlq:requeue', '--dsn=memory://', '--limit=5']));
        $this->deadLetter('third');
        self::assertSame(Application::SUCCESS, $application->run(['dlq:purge', '--dsn=memory://', '--force']));

        self::assertSame(\implode(\PHP_EOL, [
            self::ID.'  test  attempts=0  unrecoverable  2026-01-01T00:00:00+00:00  bad first',
            '1 dead letter(s).',
            '[{"id":"'.self::ID.'","channel":"test","attempts":0,"kind":"unrecoverable","failed_at":"2026-01-01T00:00:00+00:00","error":"bad first"},{"id":"0190f2a4-5b6c-7d8e-9f01-000000000002","channel":"test","attempts":0,"kind":"unrecoverable","failed_at":"2026-01-01T00:00:00+00:00","error":"bad second"}]',
            'Requeued message '.self::ID.'.',
            'Requeued 1 message(s).',
            'Purged 1 message(s).',
        ]).\PHP_EOL, $this->stdout());
        self::assertSame('Error: message '.self::ID.' is not in the dead letter queue.'.\PHP_EOL, $this->stderr());
    }

    public function testDeadLetterCommandsOnABackendWithoutDeadLetterQueue(): void
    {
        $application = $this->sharedApplication(new RecordingTransport());

        foreach ([['dlq:list'], ['dlq:requeue'], ['dlq:purge', '--force']] as $arguments) {
            self::assertSame(Application::FAILURE, $application->run([...$arguments, '--dsn=memory://']));
        }

        self::assertSame(3, substr_count($this->stderr(), 'this backend has no dead letter queue'));
    }

    public function testQueueCommands(): void
    {
        $this->send('ready');
        $this->inMemoryTransport->send(new Message(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-000000000002'), 'mail', 'later', $this->fakeClock->now()), 90_000);
        $application = $this->sharedApplication();

        self::assertSame(Application::SUCCESS, $application->run(['queue:list', '--dsn=memory://']));
        self::assertSame(Application::SUCCESS, $application->run(['queue:list', '--dsn=memory://', '--limit=1', '--json']));
        self::assertSame(Application::SUCCESS, $application->run(['queue:purge', '--dsn=memory://', '--force']));
        self::assertSame(Application::SUCCESS, $application->run(['queue:list', '--dsn=memory://']));

        self::assertSame(\implode(\PHP_EOL, [
            self::ID.'  test  ready      attempts=0  2026-01-01T00:00:00+00:00',
            '0190f2a4-5b6c-7d8e-9f01-000000000002  mail  delayed    attempts=0  2026-01-01T00:00:00+00:00  available_at=2026-01-01T00:01:30+00:00',
            '2 message(s).',
            '[{"id":"'.self::ID.'","channel":"test","state":"ready","attempts":0,"created_at":"2026-01-01T00:00:00+00:00","available_at":null}]',
            'Purged 2 message(s).',
            '0 message(s).',
        ]).\PHP_EOL, $this->stdout());
    }

    public function testQueueCommandsOnABackendWithoutBrowseAndPurge(): void
    {
        $application = $this->sharedApplication(new RecordingTransport());

        self::assertSame(Application::FAILURE, $application->run(['queue:list', '--dsn=memory://']));
        self::assertSame(Application::FAILURE, $application->run(['queue:purge', '--dsn=memory://', '--force']));

        self::assertSame(
            'Error: this backend cannot list the messages in the queue.'.\PHP_EOL.'Error: this backend cannot purge the queue.'.\PHP_EOL,
            $this->stderr(),
        );
    }

    public function testWorkRunsTheBootstrapHandlers(): void
    {
        $this->send('hello');

        $exitCode = $this->sharedApplication()->run(['work', '--dsn=memory://', '--bootstrap='.__DIR__.'/../../Fixtures/cli-bootstrap.php', '--stop-when-empty', '--timeout=0', '--batch-size=5', '--max-messages=10', '--max-runtime=60', '--memory-limit=512']);

        self::assertSame(Application::SUCCESS, $exitCode);
        self::assertSame('Worker stopped: queue_empty'.\PHP_EOL, $this->stdout());
        self::assertSame(0, $this->inMemoryTransport->stats()->ready);
        self::assertSame(0, $this->inMemoryTransport->stats()->deadLetters, 'the bootstrap registered the handler');
    }

    public function testWorkSetsUpTheQueueBeforeConsuming(): void
    {
        $transport = new class($this->inMemoryTransport) implements TransportInterface, SetupAwareInterface {
            public int $setups = 0;

            public function __construct(private readonly InMemoryTransport $inner)
            {
            }

            #[Override]
            public function setup(): void
            {
                ++$this->setups;
            }

            #[Override]
            public function send(Message $message, int $delayMs = 0): void
            {
                $this->inner->send($message, $delayMs);
            }

            #[Override]
            public function sendBatch(iterable $envelopes): void
            {
                $this->inner->sendBatch($envelopes);
            }

            #[Override]
            public function receive(int $max = 1, int $timeoutMs = 1000): array
            {
                return $this->inner->receive($max, $timeoutMs);
            }

            #[Override]
            public function ack(Delivery $delivery): void
            {
                $this->inner->ack($delivery);
            }

            #[Override]
            public function retry(Delivery $delivery, int $delayMs): void
            {
                $this->inner->retry($delivery, $delayMs);
            }

            #[Override]
            public function reject(Delivery $delivery, FailureReason $reason): void
            {
                $this->inner->reject($delivery, $reason);
            }

            #[Override]
            public function release(Delivery $delivery): void
            {
                $this->inner->release($delivery);
            }

            #[Override]
            public function ping(): void
            {
                $this->inner->ping();
            }

            #[Override]
            public function capabilities(): TransportCapabilities
            {
                return $this->inner->capabilities();
            }
        };

        self::assertSame(Application::SUCCESS, $this->sharedApplication($transport)->run(['work', '--dsn=memory://', '--bootstrap='.__DIR__.'/../../Fixtures/cli-bootstrap.php', '--stop-when-empty', '--timeout=0']));
        self::assertSame(1, $transport->setups);
    }

    public function testDsnOptionTakesPrecedenceOverTheEnvironment(): void
    {
        self::assertSame(Application::SUCCESS, $this->application(['QUEUE_DSN' => 'kafka://broker'])->run(['ping', '--dsn=memory://']));
    }

    public function testQueueNameAndPrefixReachTheFactory(): void
    {
        $configs = new ArrayObject();
        $application = new Application($this->streams(), queueFactory: static fn (LoggerInterface $logger): QueueFactoryInterface => new QueueFactory([
            new readonly class($configs) implements TransportBuilderInterface {
                /**
                 * @param ArrayObject<int, QueueConfig> $configs
                 */
                public function __construct(private ArrayObject $configs)
                {
                }

                #[Override]
                public function supports(StrategyDtoInterface $strategy): bool
                {
                    return true;
                }

                #[Override]
                public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
                {
                    $this->configs->append($config);

                    return new RecordingTransport();
                }
            },
        ]));

        $application->run(['ping', '--dsn=memory://', '--queue=orders', '--prefix=app']);
        $application->run(['ping', '--dsn=memory://']);

        self::assertSame(
            [['orders', 'app'], ['default', 'rq']],
            array_map(static fn (QueueConfig $queueConfig): array => [$queueConfig->name, $queueConfig->prefix], $configs->getArrayCopy()),
        );
    }

    public function testDeadLetterDefaultLimits(): void
    {
        for ($i = 0; $i < 101; ++$i) {
            $this->deadLetter('m'.$i, \sprintf('0190f2a4-5b6c-7d8e-9f01-%012d', $i));
        }

        $application = $this->sharedApplication();

        self::assertSame(Application::SUCCESS, $application->run(['dlq:list', '--dsn=memory://']));
        self::assertStringEndsWith('20 dead letter(s).'.\PHP_EOL, $this->stdout());
        self::assertSame(Application::SUCCESS, $application->run(['dlq:requeue', '--dsn=memory://']));
        self::assertStringEndsWith('Requeued 100 message(s).'.\PHP_EOL, $this->stdout());
    }

    public function testDeadLetterJsonKeepsSlashesAndUnicode(): void
    {
        $this->deadLetter('à/b');

        self::assertSame(Application::SUCCESS, $this->sharedApplication()->run(['dlq:list', '--dsn=memory://', '--json']));
        self::assertStringContainsString('"error":"bad à/b"', $this->stdout());
    }

    public function testUsageErrorsArePrefixed(): void
    {
        self::assertSame(Application::USAGE, $this->application()->run(['ping']));
        self::assertSame('Error: Missing --dsn (or the QUEUE_DSN environment variable).'.\PHP_EOL, $this->stderr());
    }

    /**
     * @return iterable<string, array{list<string>, int, string}>
     */
    public static function failingCommands(): iterable
    {
        yield 'unknown command' => [['deploy', '--dsn=memory://'], Application::USAGE, 'Unknown command "deploy"'];
        yield 'missing dsn' => [['ping'], Application::USAGE, 'Missing --dsn'];
        yield 'invalid dsn' => [['ping', '--dsn=kafka://broker'], Application::USAGE, 'Unsupported DSN scheme'];
        yield 'unknown strategy' => [['ping', '--dsn=memory://', '--strategy=sqs'], Application::USAGE, 'Unknown strategy "sqs": expected redis, rabbitmq, in-memory'];
        yield 'strategy mismatch' => [['ping', '--dsn=redis://:pw@h', '--strategy=rabbitmq'], Application::USAGE, '--strategy=rabbitmq does not match the DSN "redis://:***@h"'];
        yield 'purge without force' => [['dlq:purge', '--dsn=memory://'], Application::USAGE, 'confirm with --force'];
        yield 'queue purge without force' => [['queue:purge', '--dsn=memory://'], Application::USAGE, 'queue:purge deletes every ready and delayed message: confirm with --force'];
        yield 'invalid list limit' => [['queue:list', '--dsn=memory://', '--limit=0'], Application::USAGE, '--limit must be an integer >= 1'];
        yield 'invalid id' => [['dlq:requeue', '--dsn=memory://', '--id=nope'], Application::USAGE, 'Invalid message id'];
        yield 'work without bootstrap' => [['work', '--dsn=memory://'], Application::USAGE, 'requires --bootstrap'];
        yield 'missing bootstrap file' => [['work', '--dsn=memory://', '--bootstrap=/nonexistent.php'], Application::USAGE, 'not found'];
        yield 'bootstrap not returning a closure' => [['work', '--dsn=memory://', '--bootstrap='.__DIR__.'/../../Fixtures/cli-bootstrap-invalid.php'], Application::USAGE, 'must return a Closure'];
        yield 'invalid worker option' => [['work', '--dsn=memory://', '--bootstrap='.__DIR__.'/../../Fixtures/cli-bootstrap.php', '--batch-size=0'], Application::USAGE, '--batch-size must be an integer >= 1'];
        yield 'malformed arguments' => [['ping', 'extra'], Application::USAGE, 'Unexpected argument'];
    }

    /**
     * @param list<string> $arguments
     */
    #[DataProvider('failingCommands')]
    public function testFailures(array $arguments, int $exitCode, string $error): void
    {
        self::assertSame($exitCode, $this->application()->run($arguments));
        self::assertStringContainsString($error, $this->stderr());
    }

    public function testBackendErrorsExitWithOne(): void
    {
        $application = new Application($this->streams(), queueFactory: static fn (LoggerInterface $logger): QueueFactoryInterface => new QueueFactory([
            new readonly class implements TransportBuilderInterface {
                #[Override]
                public function supports(StrategyDtoInterface $strategy): bool
                {
                    return true;
                }

                #[Override]
                public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
                {
                    throw ConnectionException::unreachable('Test', 'down');
                }
            },
        ]));

        self::assertSame(Application::FAILURE, $application->run(['ping', '--dsn=memory://']));
        self::assertSame('Error: Test backend is unreachable: down'.\PHP_EOL, $this->stderr());
    }

    public function testDefaultQueueFactoryIsQueueBridge(): void
    {
        self::assertSame(Application::SUCCESS, (new Application($this->streams()))->run(['ping', '--dsn=memory://', '--queue=orders', '--prefix=app', '--strategy=in-memory', '-vv']));
        self::assertStringContainsString('[debug] Creating queue orders on in-memory.', $this->stderr());
    }

    /**
     * @param array<string, string> $environment
     */
    private function application(array $environment = []): Application
    {
        return new Application($this->streams(), new StrategyDtoFactory(), static fn (LoggerInterface $logger): QueueFactoryInterface => QueueBridge::factory($logger, signals: false), $environment);
    }

    /**
     * Ogni comando crea la coda da capo: un builder che restituisce sempre lo stesso transport conserva lo stato.
     */
    private function sharedApplication(?TransportInterface $transport = null): Application
    {
        $transport ??= $this->inMemoryTransport;
        $fakeClock = $this->fakeClock;

        return new Application($this->streams(), queueFactory: static fn (LoggerInterface $logger): QueueFactoryInterface => new QueueFactory([
            new readonly class($transport) implements TransportBuilderInterface {
                public function __construct(private TransportInterface $transport)
                {
                }

                #[Override]
                public function supports(StrategyDtoInterface $strategy): bool
                {
                    return true;
                }

                #[Override]
                public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface
                {
                    return $this->transport;
                }
            },
        ], $fakeClock, $logger));
    }

    private function streams(): Output
    {
        return new Output($this->stdout, $this->stderr);
    }

    private function send(string $text, string $id = self::ID): void
    {
        $this->inMemoryTransport->send(new Message(MessageId::fromString($id), 'test', $text, $this->fakeClock->now()));
    }

    private function deadLetter(string $text, string $id = self::ID): void
    {
        $this->send($text, $id);
        [$delivery] = $this->inMemoryTransport->receive();
        $this->inMemoryTransport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $delivery->message->id, $this->fakeClock->now(), 'bad '.$text));
    }

    private function stdout(): string
    {
        return (string) stream_get_contents($this->stdout, -1, 0);
    }

    private function stderr(): string
    {
        return (string) stream_get_contents($this->stderr, -1, 0);
    }
}
