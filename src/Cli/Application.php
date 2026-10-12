<?php

declare(strict_types=1);

namespace QueueBridge\Cli;

use Closure;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Dsn\DsnParser;
use QueueBridge\Dsn\StrategyDtoFactory;
use QueueBridge\Dsn\StrategyEnum;
use QueueBridge\Exception\QueueException;
use QueueBridge\Factory\QueueFactoryInterface;
use QueueBridge\Message\MessageId;
use QueueBridge\QueueBridge;
use QueueBridge\QueueInterface;
use QueueBridge\Transport\Capability\DeadLetterAwareInterface;
use QueueBridge\Transport\DeadLetter;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Worker\WorkerConfig;
use QueueBridge\Worker\WorkerInterface;
use Throwable;

/**
 * CLI `bin/queue-worker`. Exit code: 0 ok, 1 errore del backend, 2 errore d'uso.
 *
 * @api
 */
final readonly class Application
{
    public const int SUCCESS = 0;

    public const int FAILURE = 1;

    public const int USAGE = 2;

    private const string USAGE_TEXT = <<<'TXT'
        Usage: queue-worker <command> [options]

        Commands:
          work         Consume messages: --bootstrap=<file.php> returns a Closure(WorkerInterface): void
                       that registers the handlers.
          stats        Show queue statistics.
          ping         Check that the backend is reachable.
          queue:list   List the messages in the queue without consuming them (--limit, default 20).
          queue:purge  Delete the ready and delayed messages (requires --force).
          dlq:list     List dead letters (--limit, default 20).
          dlq:requeue  Requeue dead letters (--limit, default 100) or one message (--id=<uuid>).
          dlq:purge    Delete every dead letter (requires --force).

        Options:
          --dsn=<dsn>          Backend DSN (or the QUEUE_DSN environment variable):
                               redis[s]://[user:pass@]host[:port][/db][?version=&client=&consumer=&verify_version=&timeout=]
                               amqp[s]://user:pass@host[:port][/vhost][?prefetch=&tiers=&heartbeat=&timeout=&rpc_timeout=]
                               memory://
          --strategy=<name>    redis | rabbitmq | in-memory (must match the DSN scheme)
          --queue=<name>       Queue name (default: default)
          --prefix=<prefix>    Key/resource prefix (default: rq)
          --json               JSON output (stats, queue:list, dlq:list)
          -v | -vv             More log output (info | debug)
          Unknown options are an error.
          -h, --help           Show this help

        Worker options:
          --batch-size=<n> --timeout=<ms> --max-messages=<n> --max-runtime=<s> --memory-limit=<mb> --stop-when-empty
        TXT;

    /** Opzioni comuni a tutti i comandi. */
    private const array COMMON_OPTIONS = ['dsn', 'strategy', 'queue', 'prefix', 'help'];

    /** Opzioni proprie di ogni comando. */
    private const array COMMAND_OPTIONS = [
        'work' => ['bootstrap', 'batch-size', 'timeout', 'max-messages', 'max-runtime', 'memory-limit', 'stop-when-empty'],
        'stats' => ['json'],
        'ping' => [],
        'queue:list' => ['limit', 'json'],
        'queue:purge' => ['force'],
        'dlq:list' => ['limit', 'json'],
        'dlq:requeue' => ['limit', 'id'],
        'dlq:purge' => ['force'],
    ];

    /** @var Closure(LoggerInterface): QueueFactoryInterface */
    private Closure $queueFactory;

    /**
     * Complessità: O(1).
     *
     * @param ?Closure(LoggerInterface): QueueFactoryInterface $queueFactory default: QueueBridge::factory()
     * @param array<string, string>                            $environment  variabili d'ambiente (QUEUE_DSN)
     */
    public function __construct(
        private Output $output,
        private StrategyDtoFactory $strategyDtoFactory = new StrategyDtoFactory(),
        ?Closure $queueFactory = null,
        private array $environment = [],
    ) {
        $this->queueFactory = $queueFactory ?? static fn (LoggerInterface $logger): QueueFactoryInterface => QueueBridge::factory($logger);
    }

    /**
     * Complessità: O(a) per il parsing, a = numero di argomenti; il resto è quello del comando (es. `work` gira finché
     * un limite non lo ferma, `dlq:list` è O(limit)).
     *
     * @param list<string> $arguments argomenti senza il nome dello script
     */
    public function run(array $arguments): int
    {
        try {
            $input = Input::parse($arguments);
            if (null === $input->command || $input->flag('help')) {
                $this->output->line(self::USAGE_TEXT);

                return null === $input->command && !$input->flag('help') ? self::USAGE : self::SUCCESS;
            }

            $options = self::COMMAND_OPTIONS[$input->command] ?? throw new UsageException(\sprintf('Unknown command "%s". Run with --help for the list of commands.', $input->command));
            $input->assertOptions($input->command, [...self::COMMON_OPTIONS, ...$options]);
            $logger = new ConsoleLogger($this->output, $input->verbosity);

            return match ($input->command) {
                'work' => $this->work($input, $logger),
                'stats' => $this->stats($input, $logger),
                'ping' => $this->ping($input, $logger),
                'queue:list' => $this->queueList($input, $logger),
                'queue:purge' => $this->queuePurge($input, $logger),
                'dlq:list' => $this->deadLetterList($input, $logger),
                'dlq:requeue' => $this->deadLetterRequeue($input, $logger),
                default => $this->deadLetterPurge($input, $logger),
            };
        } catch (InvalidArgumentException $invalidArgumentException) {
            $this->output->error('Error: '.$invalidArgumentException->getMessage());

            return self::USAGE;
        } catch (QueueException $queueException) {
            $this->output->error('Error: '.$queueException->getMessage());

            return self::FAILURE;
        }
    }

    private function work(Input $input, LoggerInterface $logger): int
    {
        $bootstrap = $input->value('bootstrap') ?? throw new UsageException('The work command requires --bootstrap=<file.php>.');
        if (!is_file($bootstrap)) {
            throw new UsageException(\sprintf('Bootstrap file "%s" not found.', $bootstrap));
        }

        // Opzioni e DSN prima del bootstrap: un errore d'uso non deve eseguire il codice dell'applicazione.
        $workerConfig = new WorkerConfig(
            batchSize: $input->int('batch-size', 10) ?? 10,
            receiveTimeoutMs: $input->int('timeout', 1000, 0) ?? 1000,
            maxMessages: $input->int('max-messages', default: null),
            maxRuntimeSeconds: $input->int('max-runtime', default: null),
            memoryLimitMb: $input->int('memory-limit', default: null),
            stopWhenEmpty: $input->flag('stop-when-empty'),
        );
        $queue = $this->queue($input, $logger);

        $configure = $this->bootstrap($bootstrap);
        $queue->setup();

        $worker = $queue->worker($workerConfig);

        try {
            $configure($worker);
        } catch (InvalidArgumentException|QueueException $exception) {
            throw $exception;
        } catch (Throwable $throwable) {
            throw new UsageException(\sprintf('Bootstrap file "%s" failed to register the handlers: %s', $bootstrap, $throwable->getMessage()), 0, $throwable);
        }

        $this->output->line('Worker stopped: '.$worker->run()->value);

        return self::SUCCESS;
    }

    /**
     * @throws UsageException se il file non si carica o non restituisce una Closure
     *
     * @return Closure(WorkerInterface): mixed
     */
    private function bootstrap(string $file): Closure
    {
        try {
            $configure = (static fn (string $file): mixed => require $file)($file);
        } catch (Throwable $throwable) {
            throw new UsageException(\sprintf('Bootstrap file "%s" failed: %s', $file, $throwable->getMessage()), 0, $throwable);
        }

        if (!$configure instanceof Closure) {
            throw new UsageException(\sprintf('Bootstrap file "%s" must return a Closure(WorkerInterface): void.', $file));
        }

        return $configure;
    }

    private function stats(Input $input, LoggerInterface $logger): int
    {
        $queueStats = $this->queue($input, $logger)->stats();
        if (!$queueStats instanceof QueueStats) {
            $this->output->error('Error: this backend does not provide statistics.');

            return self::FAILURE;
        }

        $values = [
            'ready' => $queueStats->ready,
            'in_flight' => $queueStats->inFlight,
            'delayed' => $queueStats->delayed,
            'dead_letters' => $queueStats->deadLetters,
            'lag' => $queueStats->lag,
            'consumers' => $queueStats->consumers,
        ];

        if ($input->flag('json')) {
            $this->output->line(json_encode($values, \JSON_THROW_ON_ERROR));

            return self::SUCCESS;
        }

        foreach ($values as $name => $value) {
            $this->output->line(\sprintf('%-13s %s', $name, $value ?? 'n/a'));
        }

        return self::SUCCESS;
    }

    private function ping(Input $input, LoggerInterface $logger): int
    {
        $this->queue($input, $logger)->ping();
        $this->output->line('OK');

        return self::SUCCESS;
    }

    private function queueList(Input $input, LoggerInterface $logger): int
    {
        $queued = $this->queue($input, $logger)->browse($input->int('limit', 20) ?? 20);
        if (null === $queued) {
            $this->output->error('Error: this backend cannot list the messages in the queue.');

            return self::FAILURE;
        }

        $rows = array_map(static fn (QueuedMessage $queuedMessage): array => [
            'id' => $queuedMessage->message->id->value,
            'channel' => $queuedMessage->message->channel,
            'state' => $queuedMessage->state->value,
            'attempts' => $queuedMessage->message->attempts,
            'created_at' => $queuedMessage->message->createdAt->format(\DATE_ATOM),
            'available_at' => $queuedMessage->availableAt?->format(\DATE_ATOM),
        ], $queued);

        if ($input->flag('json')) {
            $this->output->line(json_encode($rows, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES));

            return self::SUCCESS;
        }

        foreach ($rows as $row) {
            $this->output->line(\sprintf('%s  %s  %-9s  attempts=%d  %s%s', $row['id'], $row['channel'], $row['state'], $row['attempts'], $row['created_at'], null === $row['available_at'] ? '' : '  available_at='.$row['available_at']));
        }

        $this->output->line(\sprintf('%d message(s).', \count($rows)));

        return self::SUCCESS;
    }

    private function queuePurge(Input $input, LoggerInterface $logger): int
    {
        if (!$input->flag('force')) {
            throw new UsageException('queue:purge deletes every ready and delayed message: confirm with --force.');
        }

        $purged = $this->queue($input, $logger)->purge();
        if (null === $purged) {
            $this->output->error('Error: this backend cannot purge the queue.');

            return self::FAILURE;
        }

        $this->output->line(\sprintf('Purged %d message(s).', $purged));

        return self::SUCCESS;
    }

    private function deadLetterList(Input $input, LoggerInterface $logger): int
    {
        $deadLetters = $this->deadLetters($input, $logger);
        if (!$deadLetters instanceof DeadLetterAwareInterface) {
            return self::FAILURE;
        }

        $rows = array_map(static fn (DeadLetter $deadLetter): array => [
            'id' => $deadLetter->message->id->value,
            'channel' => $deadLetter->message->channel,
            'attempts' => $deadLetter->message->attempts,
            'kind' => $deadLetter->reason->kind->value,
            'failed_at' => $deadLetter->reason->failedAt->format(\DATE_ATOM),
            'error' => $deadLetter->reason->errorMessage,
        ], $deadLetters->deadLetters($input->int('limit', 20) ?? 20));

        if ($input->flag('json')) {
            $this->output->line(json_encode($rows, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES));

            return self::SUCCESS;
        }

        foreach ($rows as $row) {
            $this->output->line(\sprintf('%s  %s  attempts=%d  %s  %s  %s', $row['id'], $row['channel'], $row['attempts'], $row['kind'], $row['failed_at'], $row['error']));
        }

        $this->output->line(\sprintf('%d dead letter(s).', \count($rows)));

        return self::SUCCESS;
    }

    private function deadLetterRequeue(Input $input, LoggerInterface $logger): int
    {
        $id = $input->value('id');
        $messageId = null === $id ? null : MessageId::fromString($id);
        $limit = $input->int('limit', 100) ?? 100;

        $deadLetters = $this->deadLetters($input, $logger);
        if (!$deadLetters instanceof DeadLetterAwareInterface) {
            return self::FAILURE;
        }

        if ($messageId instanceof MessageId) {
            if (!$deadLetters->requeueDeadLetter($messageId)) {
                $this->output->error(\sprintf('Error: message %s is not in the dead letter queue.', $messageId->value));

                return self::FAILURE;
            }

            $this->output->line(\sprintf('Requeued message %s.', $messageId->value));

            return self::SUCCESS;
        }

        $this->output->line(\sprintf('Requeued %d message(s).', $deadLetters->requeueDeadLetters($limit)));

        return self::SUCCESS;
    }

    private function deadLetterPurge(Input $input, LoggerInterface $logger): int
    {
        if (!$input->flag('force')) {
            throw new UsageException('dlq:purge deletes every dead letter: confirm with --force.');
        }

        $deadLetters = $this->deadLetters($input, $logger);
        if (!$deadLetters instanceof DeadLetterAwareInterface) {
            return self::FAILURE;
        }

        $this->output->line(\sprintf('Purged %d message(s).', $deadLetters->purgeDeadLetters()));

        return self::SUCCESS;
    }

    private function deadLetters(Input $input, LoggerInterface $logger): ?DeadLetterAwareInterface
    {
        $deadLetters = $this->queue($input, $logger)->deadLetters();
        if (!$deadLetters instanceof DeadLetterAwareInterface) {
            $this->output->error('Error: this backend has no dead letter queue.');
        }

        return $deadLetters;
    }

    private function queue(Input $input, LoggerInterface $logger): QueueInterface
    {
        $dsn = $input->value('dsn') ?? $this->environment['QUEUE_DSN'] ?? throw new UsageException('Missing --dsn (or the QUEUE_DSN environment variable).');

        $strategy = $input->value('strategy');
        if (null !== $strategy) {
            $expected = StrategyEnum::tryFrom($strategy) ?? throw new UsageException(\sprintf('Unknown strategy "%s": expected %s.', $strategy, \implode(', ', array_map(static fn (StrategyEnum $strategyEnum): string => $strategyEnum->value, StrategyEnum::cases()))));
            if (DsnParser::parse($dsn)->strategy !== $expected) {
                throw new UsageException(\sprintf('--strategy=%s does not match the DSN "%s".', $strategy, DsnParser::redact($dsn)));
            }
        }

        return ($this->queueFactory)($logger)->create(
            $this->strategyDtoFactory->fromDsn($dsn),
            new QueueConfig(name: $input->value('queue') ?? 'default', prefix: $input->value('prefix') ?? 'rq'),
        );
    }
}
