<?php

declare(strict_types=1);

/*
 * Worker started from your own PHP code (no bin/queue-worker): handlers that decode the payload by hand, middleware,
 * event listener for metrics, graceful shutdown on SIGTERM/SIGINT and two queues consumed in one process.
 * The orders are read from examples/data/orders.json.
 *
 *   php examples/08-worker-in-code.php [memory|redis|predis|rabbitmq]
 */

use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\RetryableException;
use QueueBridge\Exception\UnrecoverableMessageException;
use QueueBridge\Idempotency\InMemoryIdempotencyStore;
use QueueBridge\Message\Message;
use QueueBridge\Middleware\IdempotencyMiddleware;
use QueueBridge\Middleware\LoggingMiddleware;
use QueueBridge\QueueBridge;
use QueueBridge\Retry\FixedDelay;
use QueueBridge\System\NullSignalHandler;
use QueueBridge\System\PcntlSignalHandler;
use QueueBridge\System\SystemClock;
use QueueBridge\Text\DecodedText;
use QueueBridge\Worker\HandlerInterface;
use QueueBridge\Worker\MultiQueueWorker;
use QueueBridge\Worker\WorkerConfig;
use QueueBridgeExamples\Backend;
use QueueBridgeExamples\EventCounter;
use QueueBridgeExamples\StdoutLogger;

require __DIR__.'/../vendor/autoload.php';
require_once __DIR__.'/Support/Backend.php';
require_once __DIR__.'/Support/EventCounter.php';
require_once __DIR__.'/Support/StdoutLogger.php';

/**
 * Handler as a class: decodes the JSON itself instead of using DecodedText, and validates the fields.
 */
final readonly class OrderHandler implements HandlerInterface
{
    /**
     * @param Closure(string): void $notify
     */
    public function __construct(private Closure $notify)
    {
    }

    #[Override]
    public function handle(Message $message, DecodedText $text): void
    {
        try {
            $order = json_decode($message->text, associative: true, depth: 16, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $jsonException) {
            throw new UnrecoverableMessageException('Order is not valid JSON.', previous: $jsonException);
        }

        if (!is_array($order) || !is_int($order['id'] ?? null) || !is_array($order['items'] ?? null)) {
            throw new UnrecoverableMessageException('Order without id or items.');
        }

        if ([] === $order['items']) {
            // RetryableException can carry its own delay; after maxAttempts the message goes to the DLQ.
            throw new RetryableException(sprintf('Order %d has no items yet.', $order['id']), delayMs: 0);
        }

        printf("  order %d: %d item(s), total %s\n", $order['id'], count($order['items']), is_string($order['total'] ?? null) ? $order['total'] : '?');
        ($this->notify)(sprintf('Your order %d has been received.', $order['id']));
    }
}

$backend = Backend::fromCommandLine();
$logger = new StdoutLogger();
$factory = QueueBridge::factory(logger: $logger);

$orders = $factory->create($backend, new QueueConfig(name: 'example-orders', prefix: 'examples', maxAttempts: 2));
// One DTO (one connection) per queue: on RabbitMQ every queue gets its own channel.
$emails = $factory->create(Backend::fromCommandLine(), new QueueConfig(name: 'example-emails', prefix: 'examples'));
$orders->setup();
$emails->setup();

$json = file_get_contents(__DIR__.'/data/orders.json');
$list = json_decode(false === $json ? '[]' : $json, associative: true, depth: 16, flags: JSON_THROW_ON_ERROR);
foreach (is_array($list) ? $list : [] as $order) {
    $orders->producer()->publish('order.created', json_encode($order, JSON_THROW_ON_ERROR));
}

$orders->producer()->publish('order.created', '<xml>legacy</xml>');

$metrics = new EventCounter();
$innerConfig = new WorkerConfig(
    receiveTimeoutMs: 100, // short: with several queues an empty one must not block the others
    retryStrategy: new FixedDelay(0),
    middlewares: [
        new LoggingMiddleware($logger),
        // At-least-once: skip a message already processed. Use RedisIdempotencyStore::create() across processes.
        new IdempotencyMiddleware(new InMemoryIdempotencyStore(new SystemClock())),
    ],
    listeners: [$metrics],
);

$ordersWorker = $orders->worker($innerConfig)->on('order.created', new OrderHandler(
    static function (string $text) use ($emails): void {
        $emails->producer()->publish('email.send', $text);
    },
));
$emailsWorker = $emails->worker($innerConfig)->on('email.send', static function (Message $message): void {
    printf("  email: %s\n", $message->text);
});

$multi = new MultiQueueWorker(
    [$ordersWorker, $emailsWorker],
    new WorkerConfig(maxRuntimeSeconds: 60, stopWhenEmpty: true),
    // Ctrl+C or SIGTERM: the current message is completed, the ones already received go back to the queue.
    signalHandler: extension_loaded('pcntl') ? new PcntlSignalHandler() : new NullSignalHandler(),
    logger: $logger,
);

printf("Multi-queue worker stopped: %s\n", $multi->run()->value);
ksort($metrics->counts);
printf("Events: %s\n", json_encode($metrics->counts, JSON_THROW_ON_ERROR));

foreach ([$orders, $emails] as $queue) {
    $queue->deadLetters()?->purgeDeadLetters();
}
