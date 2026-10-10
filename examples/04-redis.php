<?php

declare(strict_types=1);

/*
 * Redis integration: connection with automatic reconnection, stable consumer name, idempotency store on Redis,
 * batch publishing, delayed messages, statistics and dead letter queue.
 *
 *   php examples/04-redis.php            # ext-redis
 *   php examples/04-redis.php predis     # predis/predis
 *
 * REDIS_HOST / REDIS_PORT / REDIS_DB select the server (default: the Redis of docker-compose.yml, 127.0.0.1:16379).
 */
use QueueBridgeExamples\Backend;
use QueueBridgeExamples\StdoutLogger;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\UnrecoverableMessageException;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\OutgoingMessage;
use snipershady\Queue\Middleware\IdempotencyMiddleware;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
use snipershady\Queue\Transport\Redis\RedisIdempotencyStore;
use snipershady\Queue\Transport\Redis\StrategyDtoRedis;
use snipershady\Queue\Worker\WorkerConfig;

require __DIR__.'/../vendor/autoload.php';
require_once __DIR__.'/Support/Backend.php';
require_once __DIR__.'/Support/StdoutLogger.php';

$strategy = Backend::strategy('predis' === ($argv[1] ?? null) ? 'predis' : 'redis');
if (!$strategy instanceof StrategyDtoRedis) {
    throw new LogicException('Expected a Redis strategy.');
}

// In production pass a stable consumer name (host or pod name): a crashed consumer's messages are recognised
// as orphans and handed to the others after claimIdleMs.
$strategy = new StrategyDtoRedis($strategy->client, consumer: gethostname().'-'.getmypid());

$config = new QueueConfig(
    name: 'example-redis',
    prefix: 'examples',
    maxAttempts: 3,
    claimIdleMs: 60_000, // must exceed the slowest handler
);

$logger = new StdoutLogger();
$queue = QueueBridge::factory(logger: $logger)->create($strategy, $config);
$queue->setup(); // creates the consumer group; idempotent

$producer = $queue->producer();
$ids = $producer->publishBatch([
    new OutgoingMessage('orders', '{"id":1,"total":"10.00"}'),
    new OutgoingMessage('orders', '{"id":2,"total":"20.00"}'),
    new OutgoingMessage('orders', 'not json'),
]);
$producer->publish('orders', '{"id":3,"total":"30.00"}', delayMs: 1500);

printf("Published %d messages, one delayed by 1.5 s. Stats: %s\n", count($ids) + 1, json_encode($queue->stats(), JSON_THROW_ON_ERROR));

// Idempotency on Redis: a message processed once is skipped if it is delivered again (at-least-once).
$idempotency = new IdempotencyMiddleware(RedisIdempotencyStore::create($strategy->client, $config), lockTtlMs: 60_000);

$worker = $queue->worker(new WorkerConfig(receiveTimeoutMs: 500, stopWhenEmpty: true, middlewares: [$idempotency]))
    ->on('orders', static function (Message $message, DecodedText $text): void {
        if (!is_array($text->value)) {
            throw new UnrecoverableMessageException('Orders must be JSON objects.');
        }

        printf("  order %s: total %s\n", json_encode($text->value['id'] ?? null, JSON_THROW_ON_ERROR), json_encode($text->value['total'] ?? null, JSON_THROW_ON_ERROR));
    });

$worker->run();
echo "Waiting for the delayed message...\n";
usleep(1_600_000);
$worker->run();

$deadLetters = $queue->deadLetters();
if ($deadLetters instanceof DeadLetterAwareInterface) {
    foreach ($deadLetters->deadLetters() as $deadLetter) {
        printf("Dead letter %s: %s (%s)\n", $deadLetter->message->id->value, $deadLetter->reason->errorMessage, $deadLetter->reason->kind->value);
    }

    printf("Purged %d dead letters.\n", $deadLetters->purgeDeadLetters());
}

printf("Stats: %s\n", json_encode($queue->stats(), JSON_THROW_ON_ERROR));
