# queue-bridge

**English** | [Italiano](README_IT.md)

[![Latest Stable Version](https://img.shields.io/packagist/v/snipershady/queue-bridge.svg)](https://packagist.org/packages/snipershady/queue-bridge)
[![Total Downloads](https://img.shields.io/packagist/dt/snipershady/queue-bridge.svg)](https://packagist.org/packages/snipershady/queue-bridge/stats)
[![Monthly Downloads](https://img.shields.io/packagist/dm/snipershady/queue-bridge.svg)](https://packagist.org/packages/snipershady/queue-bridge/stats)
[![PHP Version](https://img.shields.io/packagist/dependency-v/snipershady/queue-bridge/php.svg)](https://packagist.org/packages/snipershady/queue-bridge)
[![License](https://img.shields.io/packagist/l/snipershady/queue-bridge.svg)](https://packagist.org/packages/snipershady/queue-bridge)
[![CI](https://github.com/snipershady/queue-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/snipershady/queue-bridge/actions/workflows/ci.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-max-brightgreen.svg)](phpstan.neon)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)](tools/check-coverage.php)

Backend-agnostic message queue for PHP 8.3+: one typed API for **Redis Streams** and **RabbitMQ**, with retries, a dead
letter queue, delayed messages and automatic Redis version detection. An **InMemory** backend with the same semantics serves
tests and local development.

```php
$queue = QueueBridge::factory()->create(StrategyDtoRedis::fromPhpRedis($redis), new QueueConfig(name: 'orders'));

$queue->producer()->publish('order.created', '{"id":1001,"total":"12.90"}');

$queue->worker()->on('order.created', static function (Message $message, DecodedText $text): void {
    // $text->value is ['id' => 1001, 'total' => '12.90']
})->run();
```

Switching to RabbitMQ changes one line: `StrategyDtoRabbitMq::fromConnection($connection)` instead of
`StrategyDtoRedis::fromPhpRedis($redis)`.

- **At-least-once delivery**: no message is lost when a worker crashes (exception, segfault, OOM, `kill -9`); at worst it is
  delivered twice.
- **Arbitrary payloads**: the text can be JSON, structured data or plain text; it is decoded safely (JSON when valid,
  otherwise the raw string) and never throws.
- **Redis 6.0 → 8.4**: the server version is detected and the best commands are used (`XACKDEL`, `XTRIM ... ACKED`,
  `XREADGROUP ... CLAIM`, `DELEX`) without changing the public API.
- **RabbitMQ 3.13 / 4.x**: quorum queues, publisher confirms, `x-delivery-limit` with at-least-once dead lettering.
- **Worker**: handlers per channel, middleware, retry strategies, poison message protection (`maxDeliveries`), message
  expiration, graceful shutdown, events for metrics, several queues in one process, and a CLI.

## Contents

- [Installation](#installation)
- [Redis, RabbitMQ or InMemory?](#redis-rabbitmq-or-inmemory)
- [Integration](#integration): [Redis](#redis), [RabbitMQ](#rabbitmq), [InMemory](#inmemory), [DSN](#from-a-dsn)
- [Producing messages](#producing-messages)
- [Consuming with the Worker](#consuming-with-the-worker)
- [Consuming without the Worker](#consuming-without-the-worker)
- [CLI](#cli)
- [Examples](#examples)
- [Configuration](#configuration)
- [Recommended server configuration](#recommended-server-configuration)
- [Delivery guarantees and limits](#delivery-guarantees-and-limits)
- [Redis version support](#redis-version-support)
- [Benchmark](#benchmark)
- [Extending](#extending)
- [Development](#development)

The full design documentation (queue concepts, UML diagrams, concurrency, worker scheduling) is in [DOC.md](DOC.md).

## Installation

```bash
composer require snipershady/queue-bridge
```

Package page: [packagist.org/packages/snipershady/queue-bridge](https://packagist.org/packages/snipershady/queue-bridge) ·
Source: [github.com/snipershady/queue-bridge](https://github.com/snipershady/queue-bridge) ·
Releases: [CHANGELOG.md](CHANGELOG.md)

Then install the client of the backend you use:

| Backend | Client |
|---|---|
| Redis | `ext-redis` (recommended) or `composer require predis/predis` |
| RabbitMQ | `composer require php-amqplib/php-amqplib` |
| InMemory | nothing |
| Graceful shutdown on SIGTERM/SIGINT, handler timeout | `ext-pcntl` |

Requirements: PHP 8.3, 8.4 or 8.5; Redis >= 6.0 (Valkey and managed services reporting `redis_version` 7.2 work as
Redis 7.0); RabbitMQ 3.13 or 4.x.

## Redis, RabbitMQ or InMemory?

All three backends implement the same contract and pass the same contract tests: the application code does not change.
They differ in how the guarantees are obtained and in what they cost to run.

| | Redis (Streams) | RabbitMQ (quorum queues) | InMemory |
|---|---|---|---|
| Persistence | AOF (`appendfsync everysec`: up to ~1 s of writes lost if the server crashes) | on disk, fsync before the publisher confirm | none: lost when the process exits |
| High availability | replica + Sentinel, or Redis Cluster (keys use hash tags) | Raft replication across 3+ nodes | — |
| Backlog size | must fit in RAM | on disk, beyond RAM | process memory |
| Worker crash recovery | after `claimIdleMs` (default 5 min) | immediate, when the connection drops | after `claimIdleMs` |
| Delayed messages | exact (sorted set) | wait queues with fixed tiers, precision ≈ one tier | exact, driven by a PSR-20 clock |
| Poison message protection | `maxDeliveries` | `maxDeliveries` + broker `x-delivery-limit` | `maxDeliveries` |
| Throughput (one process, batch publish / consume) | ~115,000 / ~30,000 msg/s | ~25,000 / ~6,000 msg/s | ~330,000 / ~68,000 msg/s |
| Statistics | ready, in flight, delayed, DLQ, lag (>= 7.0), consumers | ready, in flight (this consumer), delayed, DLQ, consumers | ready, in flight, delayed, DLQ, lag |
| Idempotency store | `RedisIdempotencyStore` on the same server | needs Redis or your own store | `InMemoryIdempotencyStore` |
| Listing the queue (`browse`) | in flight, ready and delayed, read-only | ready messages only, read with `basic.get` + requeue | in flight, ready and delayed |
| Typical use | high throughput, low latency, Redis already in the stack | durability and replication first, existing RabbitMQ | unit tests, local development |

| Backend | Advantages | Disadvantages |
|---|---|---|
| **Redis** | Highest throughput and lowest latency; exact delays; one component often already present (cache, sessions); idempotency store and queue on the same server; consumer lag available from 7.0; tested from 6.0 to 8.4 with version-specific optimizations | Durability depends on the AOF configuration; the backlog must fit in memory and `maxmemory-policy` must be `noeviction`; a crashed worker's messages wait `claimIdleMs` before going to another worker; HA requires Sentinel or Cluster |
| **RabbitMQ** | Strongest durability: replicated quorum queues and publisher confirms; unacknowledged messages come back immediately when a worker dies; the broker enforces a delivery limit even if the worker cannot; backlog larger than RAM; management UI and mature operations tooling | Lower throughput per process; delays rounded to retry tiers; one more piece of infrastructure (three nodes for HA); heartbeat and `consumer_timeout` limit handler duration; no consumer lag statistic; idempotency needs an external store |
| **InMemory** | No services; deterministic with a hand-moved clock (delays, retries and expirations without waiting); full semantics, it is the reference implementation of the contract; fastest | Not persistent and not shared between processes: for tests and development only |

## Integration

`QueueBridge::factory()` returns a factory that receives **one typed DTO** per backend and a `QueueConfig`. Everything
else (producer, worker, handlers, middleware) is the same on every backend.

### Redis

```php
use QueueBridge\Config\QueueConfig;
use QueueBridge\QueueBridge;
use QueueBridge\Transport\Redis\RedisIdempotencyStore;
use QueueBridge\Transport\Redis\StrategyDtoRedis;

// ext-redis: a factory (instead of an instance) lets the client reconnect after a network failure.
$strategy = StrategyDtoRedis::fromPhpRedis(static function (): Redis {
    $redis = new Redis();
    $redis->connect('127.0.0.1', 6379, 2.0);
    $redis->auth(['app', 'secret']); // ACL user, optional

    return $redis;
});

// or Predis, which reconnects on its own
$strategy = StrategyDtoRedis::fromPredis(new Predis\Client('tcp://127.0.0.1:6379'));

// In production give each process a stable consumer name (host or pod name).
$strategy = new StrategyDtoRedis($strategy->client, consumer: gethostname());

$config = new QueueConfig(name: 'orders', claimIdleMs: 60_000);
$queue = QueueBridge::factory(logger: $logger)->create($strategy, $config);
$queue->setup(); // creates the consumer group; idempotent

// Idempotency store on the same Redis, for IdempotencyMiddleware.
$store = RedisIdempotencyStore::create($strategy->client, $config);
```

The Redis version is detected from `INFO server`. If the ACL forbids `INFO`, pass it: `StrategyDtoRedis::fromPhpRedis($redis,
RedisVersionEnum::GE70)`; an override higher than the server throws `VersionMismatchException` in `create()`.

### RabbitMQ

```php
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use QueueBridge\Transport\RabbitMq\RetryTiers;
use QueueBridge\Transport\RabbitMq\StrategyDtoRabbitMq;

// Connection factory: the connector reopens the channel after every AMQP error and the connection after a drop.
$strategy = StrategyDtoRabbitMq::fromConnectionFactory(
    static fn (): AbstractConnection => new AMQPStreamConnection('rabbitmq', 5672, 'app', 'secret'),
);

// Custom retry tiers and prefetch.
$strategy = new StrategyDtoRabbitMq(
    new PhpAmqpLibConnector(static fn (): AbstractConnection => new AMQPStreamConnection('rabbitmq', 5672, 'app', 'secret')),
    retryTiers: new RetryTiers([1_000, 10_000, 60_000, 600_000]),
    prefetch: 50,
);

$queue = QueueBridge::factory()->create($strategy, new QueueConfig(name: 'orders'));
$queue->setup(); // exchange, quorum queue, wait queues and DLQ; idempotent
```

`StrategyDtoRabbitMq::fromConnection($connection)` and `fromPhpAmqpLib($channel)` reuse an existing connection.
Publisher confirms are always on: retry, reject and requeue acknowledge the original only after the broker has confirmed
the copy, so they cannot be turned off. When you build the connection yourself, give it a `channel_rpc_timeout`
(`AMQPConnectionConfig::setChannelRPCTimeout()`): without one, a network that silently drops packets makes `stats()` or
`deadLetters()` wait forever (the DSN sets 10 s).

### InMemory

```php
use QueueBridge\Transport\InMemory\StrategyDtoInMemory;

$clock = new MyTestClock(); // any PSR-20 clock you can move forward
$queue = QueueBridge::factory(clock: $clock)->create(new StrategyDtoInMemory($clock), new QueueConfig(name: 'orders'));

$queue->producer()->publish('mail', 'welcome', delayMs: 60_000);
$clock->advance(60_000); // the delayed message is now due, without waiting
```

### From a DSN

Handy for configuration files and environment variables:

```php
use QueueBridge\Dsn\StrategyDtoFactory;

$queue = QueueBridge::factory()->create((new StrategyDtoFactory())->fromDsn(getenv('QUEUE_DSN')), new QueueConfig(name: 'orders'));
```

```
redis://[user:password@]host[:port][/db][?options]      rediss:// for TLS
amqp://[user:password@]host[:port][/vhost][?options]    amqps:// for TLS
memory://
```

| Backend | Option | Default | Meaning |
|---|---|---|---|
| Redis | `client` | `phpredis` if `ext-redis` is loaded, else `predis` | `phpredis` or `predis` |
| Redis | `version` | detected | force a level: `6.0`, `6.2`, `7.0`, `8.0`, `8.2`, `8.4` |
| Redis | `verify_version` | `true` | refuse an override higher than the server |
| Redis | `consumer` | random | consumer name in the group |
| Redis | `timeout` | `2.0` | connection timeout in seconds |
| RabbitMQ | `prefetch` | `10` | `basic_qos` prefetch count |
| RabbitMQ | `tiers` | `1000,5000,30000,120000,600000,3600000` | retry/delay tiers in ms |
| RabbitMQ | `heartbeat` | `0` | heartbeat in seconds |
| RabbitMQ | `timeout` | `3.0` | connection timeout in seconds |
| RabbitMQ | `rpc_timeout` | `10.0` | maximum wait for a broker reply, in seconds (the socket read/write timeout is raised to match) |

Unknown options are an error, so a typo (`?verison=8.0`) is not silently ignored; `memory://` takes no host or option.
A Redis user needs a password (an ACL user with `nopass` accepts any). Credentials must be URL-encoded. Boolean options accept `1/0`, `true/false`, `yes/no`, `on/off`. The AMQP vhost defaults
to `/`. Example: `redis://app:secret@cache:6379/2?consumer=web-1&timeout=1.5`.

## Producing messages

```php
$producer = $queue->producer();

$id = $producer->publish('orders', $json);                           // returns the MessageId (UUIDv7)
$producer->publish('emails', $text, headers: ['tenant' => 'acme']);
$producer->publish('reminders', $text, delayMs: 60_000);            // not delivered before one minute
$producer->publish('quotes', $text, ttlMs: 30_000);                 // not handled if older than 30 s

$ids = $producer->publishBatch([
    new OutgoingMessage('orders', $first),
    new OutgoingMessage('orders', $second, delayMs: 5_000),
]);
```

- `text` is any valid UTF-8 string up to `maxTextBytes` (default 1 MiB): JSON, structured data or plain text.
- Channel names match `^[A-Za-z0-9._:-]+$` (max 255 characters).
- `publishBatch()` uses one pipeline on Redis and batched confirms on RabbitMQ. A partial failure throws
  `BatchSendException` with the sent and the failed IDs. The messages are all read and encoded before anything is sent:
  an error while iterating the input sends nothing. With Predis the outcome of each command is reported whatever the
  client's `exceptions` option.
- Producer middleware: `CorrelationIdMiddleware` adds `correlation_id`/`causation_id` from the message being handled,
  `TraceContextMiddleware` adds a W3C `traceparent`. Pass them to `QueueBridge::factory(producerMiddlewares: [...])`.

## Consuming with the Worker

```php
$worker = $queue->worker(new WorkerConfig(
    batchSize: 10,
    receiveTimeoutMs: 1000,
    maxMessages: 10_000,        // exit after N messages,
    maxRuntimeSeconds: 3600,    // or after one hour,
    memoryLimitMb: 256,         // or above this memory: the supervisor restarts the process
    retryStrategy: new ExponentialBackoff(),
    middlewares: [new LoggingMiddleware($logger), new IdempotencyMiddleware($store)],
    listeners: [$metrics],      // WorkerEventListenerInterface
));

$worker
    ->on('orders', new OrderHandler())                       // HandlerInterface
    ->on('emails', static function (Message $message, DecodedText $text): void { /* ... */ })
    ->on('*', $fallback);                                    // channels without a dedicated handler

$reason = $worker->run(); // WorkerExitReason
```

The handler receives the `Message` (id, channel, raw text, attempts, headers, timestamps) and a `DecodedText`:
`$text->isJson()`, `$text->value` (decoded JSON with objects as arrays, or the raw string) and `$text->raw`. You can also
ignore it and decode `$message->text` yourself.

| Handler outcome | Result |
|---|---|
| returns | ack |
| throws `RetryableException` (any `Throwable` with `retryAnyThrowable: true`) | retried after the strategy delay (or the exception's `delayMs`); DLQ after `maxAttempts` |
| throws `UnrecoverableMessageException`; payload in an unknown format | DLQ immediately |
| the worker crashes before the ack | delivered again; DLQ after `maxDeliveries` deliveries |
| message expired (`ttlMs`) | discarded, or DLQ with `expiredMessagePolicy: ExpiredMessagePolicy::DeadLetter` |
| no handler for the channel | DLQ, or discarded with `unknownChannelPolicy: UnknownChannelPolicy::Discard` |

- **Graceful shutdown**: on SIGTERM/SIGINT (with `ext-pcntl`) or `stop()` (declared in `WorkerInterface`) the worker finishes the current message,
  gives back the ones already received and returns `WorkerExitReason::StopRequested`. `stopWhenEmpty: true` exits at the
  first empty receive (batch jobs, cron).
- **Middleware**: `LoggingMiddleware`, `TimeoutMiddleware` (needs `ext-pcntl`; a timeout is retried),
  `IdempotencyMiddleware` (two-phase lock on the message ID with `InMemoryIdempotencyStore`,
  `RedisIdempotencyStore::create()` or your own `IdempotencyStoreInterface`, on any backend; a duplicate still being
  processed by another worker is retried when that worker's lock expires, never acknowledged),
  `CorrelationContextMiddleware`.
- **Events**: `MessageReceived`, `MessageHandled`, `MessageFailed`, `MessageRetried`, `MessageDeadLettered`,
  `MessageDiscarded`, `MessageReleased`, `WorkerStopped`.
- **Several queues in one process**: `MultiQueueWorker` runs one round of each worker in turn, so a slow queue does not
  block the others; give the inner workers a short `receiveTimeoutMs` (e.g. 100 ms).

```php
$multi = new MultiQueueWorker(
    [$ordersWorker, $emailsWorker],
    new WorkerConfig(maxRuntimeSeconds: 3600),
    signalHandler: new PcntlSignalHandler(),
);
$multi->run();
```

Dead letters, statistics and health checks:

```php
$queue->ping();                      // throws ConnectionException: use it as a readiness probe
$stats = $queue->stats();            // ready, inFlight, delayed, deadLetters, lag, consumers
$dlq = $queue->deadLetters();        // DeadLetterAwareInterface
$dlq->deadLetters(20);               // list<DeadLetter>: message + FailureReason (kind, error class, message, date)
$dlq->requeueDeadLetter($id);        // back to the queue with attempts = 0
$dlq->requeueDeadLetters(100);
$dlq->purgeDeadLetters();
```

Listing and purging the queue itself:

```php
foreach ($queue->browse(50) ?? [] as $queued) {   // null if the backend cannot list messages
    // QueuedMessage: message, state (InFlight | Ready | Delayed), availableAt (delayed messages only)
    echo $queued->message->id->value, ' ', $queued->state->value, PHP_EOL;
}

$removed = $queue->purge();                      // ready + delayed; null if the backend cannot purge
```

- `browse()` never consumes: IDs and `attempts` do not change. Order: in flight, ready, delayed by due time. It is a
  snapshot: with running workers messages can change state while you read.
- `purge()` removes the ready and delayed messages present when it is called. Messages being processed are not touched
  (they are acknowledged or come back as usual), the dead letter queue has its own `purgeDeadLetters()`, and the queue
  stays usable.
- **RabbitMQ**: AMQP cannot read a queue without consuming it, so `browse()` takes the ready messages with `basic.get` and
  gives them back with `nack` + requeue. Messages in the wait queues and those held by other consumers are not visible;
  on RabbitMQ 3.13 every browse increments `x-delivery-count` (frequent browsing pushes messages towards
  `x-delivery-limit` and the DLQ), and messages read by a `browse()` that does not reach the end of the queue go back to
  its tail. Use it for occasional inspection, not for polling.

## Consuming without the Worker

To embed the queue in an existing daemon or event loop, use the transport directly and decide yourself what to do with
each message:

```php
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Text\TextDecoder;
use QueueBridge\Transport\Capability\MaintenanceAwareInterface;

$transport = $queue->transport();
$decoder = new TextDecoder();

while ($running) {
    if ($transport instanceof MaintenanceAwareInterface) {
        $transport->maintain(); // Redis: promotes delayed messages, recovers orphans
    }

    foreach ($transport->receive(max: 10, timeoutMs: 1000) as $delivery) {
        $decoded = $decoder->decode($delivery->message->text); // never throws

        if (!$decoded->isJson() || !is_array($decoded->value)) {
            $transport->reject($delivery, new FailureReason(
                FailureKind::Unrecoverable, $delivery->message->id, new DateTimeImmutable(), 'Expected a JSON object.',
            ));

            continue;
        }

        try {
            process($decoded->value);
            $transport->ack($delivery);
        } catch (TemporaryFailure) {
            $transport->retry($delivery, 5_000); // new copy with attempts + 1, delivered in 5 s
        }
    }
}
```

Without the Worker you take over what it does for you: `maxAttempts` (`$delivery->message->attempts`), `maxDeliveries`
(`$delivery->deliveryCount`), expiration (`$delivery->message->expiresAt`), signals and limits.
`$transport->release($delivery)` gives a message back without counting a failure.

## CLI

```
vendor/bin/queue-worker <command> --dsn=<dsn> [--queue=<name>] [--prefix=<prefix>] [options]
```

| Command | What it does |
|---|---|
| `work --bootstrap=<file.php>` | runs a worker; the file returns a `Closure(WorkerInterface): void` that registers the handlers ([example](examples/03-worker-bootstrap.php)). Options: `--batch-size`, `--timeout` (ms), `--max-messages`, `--max-runtime` (s), `--memory-limit` (MB), `--stop-when-empty` |
| `stats [--json]` | queue statistics |
| `ping` | backend health check |
| `dlq:list [--limit=20] [--json]` | dead letters with the failure reason |
| `dlq:requeue [--limit=100 \| --id=<uuid>]` | requeue the dead letters (up to the limit) or one of them |
| `dlq:purge --force` | delete every dead letter |
| `queue:list [--limit=20] [--json]` | messages in the queue (in flight, ready, delayed) without consuming them |
| `queue:purge --force` | delete the ready and delayed messages |

The DSN can also come from the `QUEUE_DSN` environment variable; `--strategy=redis|rabbitmq|in-memory` is an optional check
against the DSN scheme. `-v`/`-vv` print info/debug logs on stderr. An option that the command does not accept is a usage
error (`work --max-message=5` does not run with the default limit). `work` validates its options and the DSN before loading
the bootstrap file; a bootstrap that throws is a usage error too. Exit codes: `0` success, `1` backend error, `2` usage
error.

Run workers under a supervisor with `--max-runtime` and `--memory-limit`: the process exits cleanly and is restarted.
Ready-made configurations: [systemd](examples/deploy/queue-worker.service) and
[Supervisor](examples/deploy/supervisord.conf).

## Examples

The [`examples/`](examples/) directory (left out of the Composer package) contains runnable scripts. Services and ports
are those of `docker-compose.yml` (`docker compose up -d --wait`); `REDIS_HOST`, `REDIS_PORT`, `AMQP_HOST`, `AMQP_PORT`,
`AMQP_USER`, `AMQP_PASSWORD` point them elsewhere.

| File | Shows | Run |
|---|---|---|
| [01-quick-start.php](examples/01-quick-start.php) | publish and consume, JSON and plain text | `php examples/01-quick-start.php` |
| [02-switch-backend.php](examples/02-switch-backend.php) | the same code on every backend, only the DTO changes | `php examples/02-switch-backend.php memory\|redis\|predis\|rabbitmq` |
| [03-worker-bootstrap.php](examples/03-worker-bootstrap.php) | handlers for `queue-worker work` | see the CLI section |
| [04-redis.php](examples/04-redis.php) | reconnection, consumer name, Redis idempotency store, batch, delays, DLQ | `php examples/04-redis.php [predis]` |
| [05-rabbitmq.php](examples/05-rabbitmq.php) | connection factory, retry tiers, prefetch, retry through a wait queue, DLQ | `php examples/05-rabbitmq.php` |
| [06-in-memory-tests.php](examples/06-in-memory-tests.php) | testing handlers with a hand-moved clock: delays, retries, expiration, DLQ | `php examples/06-in-memory-tests.php` |
| [07-manual-consume.php](examples/07-manual-consume.php) | consuming without the Worker: receive, manual decoding, ack/retry/reject | `php examples/07-manual-consume.php memory\|redis\|predis\|rabbitmq` |
| [08-worker-in-code.php](examples/08-worker-in-code.php) | Worker started from PHP code: handler class with manual JSON decoding, middleware, metrics, signals, two queues | `php examples/08-worker-in-code.php memory\|redis\|predis\|rabbitmq` |

Sample payloads are in [`examples/data/`](examples/data/), deployment files in [`examples/deploy/`](examples/deploy/).

## Configuration

`QueueConfig` is the same on every backend:

| Option | Default | Meaning |
|---|---|---|
| `name` | `default` | queue name |
| `prefix` | `rq` | prefix of Redis keys / RabbitMQ resources |
| `maxTextBytes` | 1 MiB | text size limit, in bytes |
| `maxHeadersBytes` | 64 KiB | headers size limit |
| `maxAttempts` | 3 | handler failures before the DLQ |
| `maxDeliveries` | 10 | deliveries (crashes included) before the DLQ; at least `maxAttempts` + 1 |
| `claimIdleMs` | 300000 | Redis: after this idle time an unacknowledged message is taken from a crashed worker |
| `dlqMaxLength` | 100000 | DLQ retention (newest kept): approximate on Redis (`MAXLEN ~`), exact on RabbitMQ (`x-max-length`) and InMemory |

Redis layout (hash tags keep the keys of a queue in one cluster slot): `rq:{orders}:stream`, `rq:{orders}:delayed`,
`rq:{orders}:dlq`, `rq:{orders}:idem:<id>`; consumer group `queue-bridge`.

RabbitMQ topology created by `setup()`: topic exchange `rq.orders` (routing key = channel), quorum queue `rq.orders` with
`x-delivery-limit`, wait queues `rq.orders.retry.<ms>`, classic DLQ `rq.orders.dlq` with `x-max-length` = `dlqMaxLength`
(overflow `drop-head`: the oldest dead letters are dropped, a reject never fails because the DLQ is full).

## Recommended server configuration

**Redis**

```
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

Without AOF a restart empties the queue; with an eviction policy Redis may delete queue keys under memory pressure. Use ACL
users limited to the queue keys (`~rq:*`); if the ACL forbids `INFO`, pass the version explicitly (otherwise the library
falls back to the 6.0 baseline and logs a warning).

**RabbitMQ**

- The library declares quorum queues: run at least three nodes in production.
- `consumer_timeout` (default 30 minutes) closes the channel when a message stays unacknowledged longer: keep handlers well
  below it.
- Heartbeats: a synchronous PHP worker does not answer heartbeats while a handler runs, so long handlers make the broker
  close the connection. Set a heartbeat larger than the longest handler, disable it (`heartbeat=0`, the DSN default) or use
  php-amqplib's `PCNTLHeartbeatSender` (`ext-pcntl`).

## Delivery guarantees and limits

- **At-least-once**: a message is acknowledged only after the handler returns. A crash between the handler and the ack, a
  network failure or an expired lock produce a **duplicate**: **handlers must be idempotent** (or use
  `IdempotencyMiddleware`). Retries and DLQ moves are atomic on Redis (Lua) and publish-confirm-then-ack on RabbitMQ: never
  lost, at worst duplicated.
- **No ordering guarantee**: retries, delays, released and recovered messages change the order (a released or recovered
  message goes behind the others, on InMemory as on Redis). Messages arrive in publishing order only in the absence of
  these events. Delayed messages become available in order of due time; on RabbitMQ only across different tiers.
- **No fan-out**: one `QueueConfig` is one queue; each message is handled by one worker. For publish/subscribe, publish to
  several queues.
- **Scaling with processes**: the worker is single-process and never forks. Run more processes, on more machines, on the
  same queue; two workers never handle the same message at the same time.
- **`claimIdleMs` and handler duration (Redis)**: a message unacknowledged for `claimIdleMs` is considered orphaned and
  given to another worker. `claimIdleMs` must be larger than the longest handler (enforce it with `TimeoutMiddleware`),
  otherwise a slow message is processed twice in parallel. The whole batch counts as in flight from the moment it is
  received: the worker starts a message only if it was received less than `claimIdleMs - handler timeout` ago
  (`claimIdleMs / 2` without `TimeoutMiddleware`) and releases the rest of the batch with a warning in the log. If the
  warning shows up, lower `batchSize`.
- **RabbitMQ**: orphans come back as soon as the connection closes; the limits are the heartbeat and `consumer_timeout`
  described above. `x-delivery-limit` equals `maxDeliveries`, so a message that keeps crashing the worker is dead-lettered by
  the broker as a last resort. On RabbitMQ 3.13 `release()` also counts as a delivery. Statistics are a snapshot: right
  after a `release()` the message may be in transit and missing from `ready` for a moment.
- **Delay precision on RabbitMQ**: delays use wait queues with fixed TTL tiers (default 1 s, 5 s, 30 s, 2 min, 10 min, 1 h).
  The largest tier not above the remaining delay is used and a message that comes back early waits again: precision is
  about one tier (1500 ms is delivered between 1.5 and 3.5 s). Beyond the largest tier the message cycles through it until
  due. Choose retry delays that match the tiers. On Redis delays are exact within the receive timeout; on InMemory they
  follow the clock.
- **Wire format**: messages are stored in a versioned format; a rolling deploy with old and new workers loses nothing.
  Unknown formats go to the DLQ and never crash the worker. See [UPGRADE.md](UPGRADE.md).

## Redis version support

The level is detected once from `INFO server`; each capability is a strategy chosen at creation, never a version check in
the hot path.

| Level | Commands used | Below this level |
|---|---|---|
| 6.0 | Streams, consumer groups, extended `XPENDING`, Lua scripts, sorted sets, `SET NX PX` | — |
| 6.2 | `XPENDING ... IDLE`, `XTRIM MINID` | client-side idle filter, no safety trim |
| 7.0 | `XINFO GROUPS` lag | `lag` = null |
| 8.0 | same as 7.0 | |
| 8.2 | `XACKDEL`, `XTRIM ... ACKED` | `XACK` + `XDEL`, `MINID` trim |
| 8.4 | `XREADGROUP ... CLAIM`, `DELEX ... IFEQ` | orphans requeued by a Lua script, lock release in Lua |

The main stream is never trimmed with `MAXLEN` (it would delete unread messages): acknowledged entries are deleted, and the
trim is only a safety net that removes acknowledged entries. The CI runs the integration tests on Redis 6.0, 6.2, 7.0,
7.4, 8.0, 8.2 and 8.4 with ext-redis and Predis, and on RabbitMQ 3.13 and 4.x, with PHP 8.3, 8.4 and 8.5.

## Benchmark

```bash
docker compose up -d --wait
# arguments: DSN, messages (default 20000), batch size (default 100); disable Xdebug for meaningful numbers
php -d xdebug.mode=off tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0' 20000 100
php -d xdebug.mode=off tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0?version=6.0'   # 6.0 strategies
php -d xdebug.mode=off tests/Benchmark/benchmark.php 'amqp://guest:guest@127.0.0.1:35672'
```

Indicative results: PHP 8.3 CLI with OPcache, Xdebug off, AMD Ryzen 9 9900X; servers in Docker on the same machine,
reached through the published ports (Redis 8.4, RabbitMQ 4.x single node); 157-byte JSON payload, batches of 100, one
process. "consume" includes receive, a no-op handler and the ack.

| Backend | publish | publishBatch | consume |
|---|---:|---:|---:|
| InMemory | 300,000 msg/s | 327,000 msg/s | 68,000 msg/s |
| Redis, ext-redis | 3,000 msg/s | 115,000 msg/s | 30,600 msg/s |
| Redis, Predis | 3,400 msg/s | 95,500 msg/s | 32,000 msg/s |
| RabbitMQ (publisher confirms) | 1,100 msg/s | 25,000 msg/s | 6,000 msg/s |

Redis levels forced on the same 8.4 server (ext-redis):

| Level | publishBatch | consume |
|---|---:|---:|
| 6.0 | 107,000 msg/s | 32,200 msg/s |
| 6.2 | 110,000 msg/s | 29,400 msg/s |
| 7.0 | 106,000 msg/s | 30,000 msg/s |
| 8.0 | 110,000 msg/s | 30,700 msg/s |
| 8.2 | 79,000 msg/s | 27,400 msg/s |
| 8.4 | 108,000 msg/s | 33,300 msg/s |

A single `publish()` is bound by the network round trip (one per message, plus the publisher confirm on RabbitMQ; here
also by Docker's port forwarding): use `publishBatch()` for throughput. Consumption scales with the number of worker
processes. The levels differ in the atomicity and round trips of maintenance operations (orphan recovery, trim), not in the
hot path, so their numbers are within run-to-run noise.

## Extending

A new backend (for example SQS) implements `StrategyDtoInterface`, `TransportInterface` (plus the optional capability
interfaces it supports: setup, maintenance, stats, dead letters, batch ack, purge, browse) and `TransportBuilderInterface`, and is registered in the factory:

```php
$factory = new QueueFactory(builders: [new SqsTransportBuilder(), new RedisTransportBuilder(), new InMemoryTransportBuilder()]);
```

The core does not change. `EnvelopeCodec` converts messages to the versioned wire format, and the abstract
`TransportContractTestCase` in the repository (`tests/Contract`) verifies the same semantics as the provided backends.

A custom client (company wrapper, framework facade) implements `RedisClientInterface` or `AmqpConnectorInterface` and is
passed to the DTO constructor.

## Development

```bash
composer qa                      # validate, Rector, php-cs-fixer, PHPStan max + architecture rules, 100% coverage
docker compose up -d --wait      # Redis on 16379, RabbitMQ on 35672 (management on 35673)
REDIS_DSN=redis://127.0.0.1:16379/15 AMQP_DSN=amqp://guest:guest@127.0.0.1:35672 composer test:integration
composer mutation                # Infection, MSI >= 85%
```

See [CHANGELOG.md](CHANGELOG.md) and [UPGRADE.md](UPGRADE.md).

## License

GPL-2.0-only.
