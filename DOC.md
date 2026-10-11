# queue-bridge: design documentation

**English** | [Italiano](DOC_IT.md)

This document explains how queue-bridge works: what a message queue is and how the library maps each concept to code,
the UML model (class, sequence and activity diagrams in Mermaid), the implemented specifications, how concurrency is
handled and how to run the workers on a server. For installation and a quick tour see the [README](README.md).

## Contents

1. [Message queues](#1-message-queues)
2. [Architecture](#2-architecture)
3. [Class diagrams](#3-class-diagrams)
4. [Sequence diagrams](#4-sequence-diagrams)
5. [Activity diagrams](#5-activity-diagrams)
6. [Specifications](#6-specifications)
7. [Concurrency](#7-concurrency)
8. [Running the worker on a server](#8-running-the-worker-on-a-server)

---

## 1. Message queues

### 1.1 What a queue is for

A message queue is a buffer between the code that **produces** work and the code that **performs** it. The producer
writes a message and returns immediately; one or more consumers read the messages later, at their own pace, possibly on
other machines. This gives three properties that a direct call cannot give:

- **Decoupling**: the producer does not know who processes the message, nor when, nor in which process.
- **Load levelling**: a burst of 10,000 requests becomes a backlog that the workers drain at the rate they can sustain,
  instead of 10,000 concurrent calls that overload the database.
- **Resilience**: if the consumer is down, messages wait in the queue; nothing is lost and nothing has to be retried by
  the caller.

Typical case: an HTTP request that must send an e-mail answers immediately and leaves the e-mail to a worker.

```php
// In the web request: publish and return.
$queue->producer()->publish('mail.welcome', json_encode(['user' => 42], JSON_THROW_ON_ERROR));

// In a separate, long-running process: consume.
$queue->worker()->on('mail.welcome', static function (Message $message, DecodedText $text): void {
    $mailer->sendWelcome($text->value['user']);
})->run();
```

### 1.2 Producer, broker, consumer, message

| Concept | Meaning | In queue-bridge |
|---|---|---|
| **Message** | a unit of work: a payload plus metadata | `Message`: UUIDv7 `id`, `channel`, `text`, `attempts`, `createdAt`, `expiresAt`, `headers` |
| **Producer** | writes messages | `ProducerInterface::publish()` / `publishBatch()` |
| **Broker** | stores messages durably and hands them out | Redis (Streams) or RabbitMQ; `InMemory` for tests |
| **Consumer** | reads and processes messages | `Worker` (`WorkerInterface`) or your own loop over `TransportInterface::receive()` |
| **Queue** | a named stream of messages on the broker | `QueueConfig::$name` (+ `$prefix`) |
| **Channel / routing key** | a label that selects the handler | `Message::$channel`, `WorkerInterface::on($channel, $handler)` |

The **channel** is not a separate queue: all the messages of a `QueueConfig` travel in the same queue, and the worker picks
the handler by channel (`*` is the fallback). Use different queues when you need separate scaling or priorities.

```php
$queue = QueueBridge::factory()->create(StrategyDtoRedis::fromPhpRedis($redis), new QueueConfig(name: 'orders'));

$queue->producer()->publish('order.created', '{"id":1001}');
$queue->producer()->publish('order.cancelled', '{"id":1002}');

$queue->worker()
    ->on('order.created', $createdHandler)
    ->on('order.cancelled', $cancelledHandler)
    ->on('*', $fallback)          // any other channel
    ->run();
```

The **payload** (`text`) is arbitrary: JSON, structured data or plain text. The library never re-encodes it and decodes it
safely for the handler: `DecodedText` is JSON (`kind: Json`, arrays instead of objects) when the text is valid JSON,
otherwise the raw string (`kind: Plain`). Decoding never throws.

### 1.3 Point-to-point and competing consumers

There are two families of messaging:

- **Point-to-point**: each message is processed by **one** consumer. Several consumers on the same queue *compete* for the
  messages, which spreads the load.
- **Publish/subscribe (fan-out)**: each message is copied to every subscriber.

queue-bridge implements **point-to-point with competing consumers**. To scale, start more worker processes on the same
queue: each message goes to only one of them. There is no fan-out: to deliver the same event to two services, publish it to
two queues.

```php
// Two processes running the same code share the work of the "orders" queue.
// php worker.php   (process 1)
// php worker.php   (process 2)
$queue->worker(new WorkerConfig(batchSize: 20))->on('order.created', $handler)->run();
```

### 1.4 Acknowledgements and delivery guarantees

A consumer that has finished a message **acknowledges** it (ack), and only then the broker deletes it. What happens if the
consumer dies before the ack defines the guarantee:

| Guarantee | Behaviour | Cost |
|---|---|---|
| at-most-once | ack *before* processing: a crash loses the message | never duplicates |
| **at-least-once** | ack *after* processing: a crash makes the message come back | may duplicate |
| exactly-once | requires a transaction spanning broker and side effects | not achievable in general |

queue-bridge is **at-least-once**: the worker acknowledges only after the handler has returned, so no message is lost, even
on a segfault, an out-of-memory error or `kill -9`. The price is that a message can be processed twice (the handler
finishes, the process dies before the ack). Handlers must therefore be **idempotent**, or protected by
`IdempotencyMiddleware`, which records the message ID in a store and skips the messages already completed (a duplicate
still being processed by another worker is retried when that worker's lock expires, never acknowledged):

```php
$store = RedisIdempotencyStore::create($redisClient, $config);   // or InMemoryIdempotencyStore, or your own

$worker = $queue->worker(new WorkerConfig(middlewares: [
    new IdempotencyMiddleware($store, lockTtlMs: 300_000, doneTtlMs: 86_400_000),
]));
```

### 1.5 In-flight messages, orphans and redelivery

Between delivery and ack a message is **in flight**: assigned to a consumer and invisible to the others. If that consumer
dies, the message becomes an **orphan** and must come back to the queue:

- **RabbitMQ** does it natively: when the connection (or channel) closes, the unacknowledged messages are requeued at once.
- **Redis** keeps in-flight entries in the consumer group's pending list (PEL). A pending entry idle for more than
  `QueueConfig::$claimIdleMs` (default 5 minutes) is copied back to the stream by the periodic maintenance of any worker
  (on Redis >= 8.4, `XREADGROUP ... CLAIM` recovers it directly while reading).
- **InMemory** behaves like Redis, with a PSR-20 clock.

`claimIdleMs` must be longer than the slowest handler, otherwise a slow but alive message is taken for an orphan and
processed twice in parallel. The worker validates this against `TimeoutMiddleware`.

A whole batch is in flight as soon as it is received, including the messages still waiting for their turn. On Redis and
InMemory the worker therefore starts a delivery only if it was received less than `claimIdleMs - handler timeout` ago
(`claimIdleMs / 2` without `TimeoutMiddleware`); the rest of the batch is returned with `release()`, after acknowledging
the messages already handled, and a warning is logged. If the warning shows up, lower `batchSize` or the handler timeout.

```php
new QueueConfig(name: 'reports', claimIdleMs: 900_000);   // handlers may take up to ~10 minutes
```

### 1.6 Retries and backoff

A handler can fail for a transient reason (timeout, a service that restarts). Retrying immediately usually fails again, so
retries are spaced with a **backoff**: 1 s, 2 s, 4 s... plus random **jitter** so that many failures do not retry at the
same instant.

In queue-bridge a handler asks for a retry by throwing `RetryableException` (optionally with its own delay), or any
`Throwable` when `retryAnyThrowable` is enabled. The worker publishes a new copy with `attempts + 1` after the delay chosen
by the `RetryStrategyInterface` (`ExponentialBackoff` by default, `FixedDelay`, `NoRetry`), then acknowledges the original:
the retry is never lost (atomic Lua script on Redis, publish-confirm-then-ack on RabbitMQ).

```php
$worker = $queue->worker(new WorkerConfig(
    retryStrategy: new ExponentialBackoff(initialMs: 1000, multiplier: 2.0, maxMs: 300_000, jitter: 0.1),
))->on('invoice.send', static function (Message $message, DecodedText $text) use ($api): void {
    try {
        $api->send($text->value);
    } catch (ServiceUnavailable $e) {
        throw new RetryableException('API unavailable', delayMs: 30_000, previous: $e);
    }
});
```

`QueueConfig::$maxAttempts` (default 3) is the number of failed attempts after which the message goes to the dead letter
queue instead of being retried.

### 1.7 Poison messages and the dead letter queue

A **poison message** is one that can never be processed: invalid data, a bug for that particular input, or a payload that
crashes the process every time. Without protection it would be retried forever and block a worker.

The **dead letter queue** (DLQ) is a separate queue where such messages are parked with the reason of the failure, for a
human or a tool to inspect, fix and requeue. A message goes to the DLQ when:

- the handler throws `UnrecoverableMessageException` (or the payload cannot be decoded: `SerializationException`);
- the handler fails and `attempts` reaches `maxAttempts`;
- the message was **delivered** more than `maxDeliveries` times (default 10) without ever being acknowledged: it makes the
  worker crash before the `catch` is reached (segfault, OOM, `kill -9`); on RabbitMQ the broker enforces the same limit
  with `x-delivery-limit`;
- it targets a channel without a handler (`UnknownChannelPolicy::DeadLetter`, the default) or it has expired
  (`ExpiredMessagePolicy::DeadLetter`).

```php
$dlq = $queue->deadLetters();                  // DeadLetterAwareInterface, null if the backend has no DLQ
foreach ($dlq->deadLetters(20) as $deadLetter) {
    printf("%s %s %s\n", $deadLetter->message->id->value, $deadLetter->reason->kind->value, $deadLetter->reason->errorMessage);
}
$dlq->requeueDeadLetter($id);                  // back to the queue with attempts = 0, same ID
$dlq->requeueDeadLetters(100);
$dlq->purgeDeadLetters();
```

### 1.8 Delayed messages and expiration

A **delayed** message becomes visible only after a given time (reminders, scheduled retries). An **expiring** message (TTL)
is worthless after a deadline (a one-time code, a price quote).

```php
$producer = $queue->producer();
$producer->publish('reminder.send', '{"user":42}', delayMs: 3_600_000);   // in one hour
$producer->publish('otp.send', '{"phone":"+39..."}', ttlMs: 60_000);      // useless after one minute
```

Redis keeps delayed messages in a sorted set (exact delays). RabbitMQ uses **wait queues** with fixed TTL tiers (1 s, 5 s,
30 s, 2 min, 10 min, 1 h by default): the delay is approximated by the tiers, and a message that comes back early is sent
back to wait. The worker checks the expiration before calling the handler and acknowledges (`Discard`, default) or
dead-letters (`DeadLetter`) the expired messages.

### 1.9 Ordering

Queues are often thought of as FIFO, but strict ordering and parallel consumers are incompatible: with two workers, message 2
can finish before message 1. Retries, delays and orphan recovery also reorder messages. queue-bridge **does not guarantee
ordering**: `receive()` returns messages in arrival order, but processing order is not guaranteed. When order matters, make
handlers commutative (e.g. compare a version number) or use a single worker per queue and no retries.

### 1.10 Throughput: batches and prefetch

Each round trip to the broker costs latency, so producers and consumers work in batches:

- `publishBatch()` sends many messages in one Redis pipeline or one series of RabbitMQ publishes with a single wait for the
  publisher confirms;
- `WorkerConfig::$batchSize` is how many messages a worker receives per round trip; acknowledgements of a batch are sent
  together (`ackBatch`);
- on RabbitMQ `prefetch` (DTO option, default 10) is how many unacknowledged messages the broker pushes to a consumer in
  advance.

```php
$queue->producer()->publishBatch([
    new OutgoingMessage('order.created', '{"id":1}'),
    new OutgoingMessage('order.created', '{"id":2}', delayMs: 5_000),
]);
```

### 1.11 Inspecting and purging a queue

Operations staff need to see what is waiting and, sometimes, to throw it away (a bad deploy that published garbage):

```php
foreach ($queue->browse(50) ?? [] as $queued) {          // list without consuming
    echo $queued->state->value, ' ', $queued->message->channel, PHP_EOL;  // in_flight | ready | delayed
}

$queue->purge();                                          // removes ready + delayed, keeps in-flight and DLQ
```

The same from the shell: `queue-worker queue:list --dsn=... --json` and `queue-worker queue:purge --dsn=... --force`.

### 1.12 Choosing a backend

The application code is the same on every backend: only the strategy DTO passed to the factory changes.

```php
$queue = $factory->create(StrategyDtoRedis::fromPhpRedis($redis), $config);          // Redis
$queue = $factory->create(StrategyDtoRabbitMq::fromConnection($connection), $config); // RabbitMQ
$queue = $factory->create(new StrategyDtoInMemory(), $config);                       // tests
```

Redis offers the highest throughput and exact delays; RabbitMQ the strongest durability (replicated quorum queues,
immediate redelivery of orphans, broker-side delivery limit); InMemory has no infrastructure and a controllable clock. The
README has a detailed comparison.

---

## 2. Architecture

queue-bridge follows the **ports and adapters** (hexagonal) architecture. The core (producer, worker, middleware, retry,
message model, codec) depends only on interfaces; each backend is an adapter in its own folder (`Transport/<Backend>`) and
never depends on another adapter. `QueueBridge`, `Dsn/` and `Cli/` are the composition root, the only code that knows the
concrete backends. These rules are verified by phpat inside PHPStan.

```mermaid
flowchart TB
    subgraph Core["Core (backend-agnostic)"]
        P["Producer + producer middleware"]
        W["Worker / MultiQueueWorker + middleware"]
        M["Message, Delivery, Envelope, QueuedMessage"]
        C["EnvelopeCodec (wire format v1)"]
        R["RetryStrategy, Idempotency, System ports"]
    end
    T{{"TransportInterface + capability interfaces (port)"}}
    subgraph Adapters
        RT["RedisTransport"]
        AT["RabbitMqTransport"]
        IT["InMemoryTransport"]
    end
    RC{{"RedisClientInterface"}}
    AC{{"AmqpConnectorInterface"}}
    P --> T
    W --> T
    T -.implemented by.-> RT
    T -.implemented by.-> AT
    T -.implemented by.-> IT
    RT --> RC
    AT --> AC
    RC -.-> PR["PhpRedisClient / PredisClient"]
    AC -.-> PA["PhpAmqpLibConnector"]
    Root["Composition root: QueueBridge, Dsn, Cli"] --> Core
    Root --> Adapters
```

Patterns: **Strategy** for Redis version differences (`Transport/Redis/Feature`), **Chain of Responsibility** for choosing the
transport builder from the DTO, **Middleware/Decorator** for cross-cutting concerns, **Template Method** for the contract
tests, **named constructors** for adapting native clients.

---

## 3. Class diagrams

### 3.1 Queue facade, factory and strategy DTOs

```mermaid
classDiagram
    direction LR
    class QueueFactoryInterface {
        <<interface>>
        +create(StrategyDtoInterface strategy, QueueConfig config) QueueInterface
    }
    class QueueFactory {
        -list~TransportBuilderInterface~ builders
        +create(StrategyDtoInterface strategy, QueueConfig config) QueueInterface
    }
    class TransportBuilderInterface {
        <<interface>>
        +supports(StrategyDtoInterface strategy) bool
        +build(StrategyDtoInterface strategy, QueueConfig config) TransportInterface
    }
    class StrategyDtoInterface {
        <<interface>>
        +name() string
    }
    class StrategyDtoRedis {
        +RedisClientInterface client
        +RedisVersionEnum version
        +bool verifyVersion
        +string consumer
        +fromPhpRedis(Redis or Closure redis)$ StrategyDtoRedis
        +fromPredis(ClientInterface predis)$ StrategyDtoRedis
    }
    class StrategyDtoRabbitMq {
        +AmqpConnectorInterface connector
        +RabbitMqDelayModeEnum delayMode
        +RetryTiers retryTiers
        +int prefetch
        +fromPhpAmqpLib(AMQPChannel channel)$ StrategyDtoRabbitMq
        +fromConnection(AbstractConnection connection)$ StrategyDtoRabbitMq
        +fromConnectionFactory(Closure factory)$ StrategyDtoRabbitMq
    }
    class StrategyDtoInMemory {
        +ClockInterface clock
    }
    class QueueInterface {
        <<interface>>
        +setup() void
        +producer() ProducerInterface
        +worker(WorkerConfig config) WorkerInterface
        +transport() TransportInterface
        +ping() void
        +stats() QueueStats
        +deadLetters() DeadLetterAwareInterface
        +purge() int
        +browse(int limit) list~QueuedMessage~
        +capabilities() TransportCapabilities
    }
    class Queue
    class QueueBridge {
        +factory(LoggerInterface logger, ClockInterface clock, array producerMiddlewares, bool signals)$ QueueFactory
    }
    QueueFactoryInterface <|.. QueueFactory
    QueueFactory o--> "1..*" TransportBuilderInterface
    TransportBuilderInterface <|.. RedisTransportBuilder
    TransportBuilderInterface <|.. RabbitMqTransportBuilder
    TransportBuilderInterface <|.. InMemoryTransportBuilder
    StrategyDtoInterface <|.. StrategyDtoRedis
    StrategyDtoInterface <|.. StrategyDtoRabbitMq
    StrategyDtoInterface <|.. StrategyDtoInMemory
    QueueInterface <|.. Queue
    QueueFactory ..> Queue : creates
    QueueBridge ..> QueueFactory : creates
    Queue --> TransportInterface
```

`stats()`, `deadLetters()`, `purge()` and `browse()` return `null` when the transport does not implement the matching
capability interface.

### 3.2 Transport port and capabilities

```mermaid
classDiagram
    direction TB
    class TransportInterface {
        <<interface>>
        +send(Message message, int delayMs) void
        +sendBatch(iterable envelopes) void
        +receive(int max, int timeoutMs) list~Delivery~
        +ack(Delivery delivery) void
        +retry(Delivery delivery, int delayMs) void
        +reject(Delivery delivery, FailureReason reason) void
        +release(Delivery delivery) void
        +ping() void
        +capabilities() TransportCapabilities
    }
    class SetupAwareInterface {
        <<interface>>
        +setup() void
    }
    class MaintenanceAwareInterface {
        <<interface>>
        +maintain() MaintenanceReport
    }
    class StatsAwareInterface {
        <<interface>>
        +stats() QueueStats
    }
    class BatchAckAwareInterface {
        <<interface>>
        +ackBatch(list deliveries) void
    }
    class DeadLetterAwareInterface {
        <<interface>>
        +deadLetters(int limit) list~DeadLetter~
        +requeueDeadLetters(int limit) int
        +requeueDeadLetter(MessageId id) bool
        +purgeDeadLetters() int
    }
    class PurgeAwareInterface {
        <<interface>>
        +purge() int
    }
    class BrowseAwareInterface {
        <<interface>>
        +browse(int limit) list~QueuedMessage~
    }
    class TransportCapabilities {
        +OrphanRecovery orphanRecovery
        +bool nativeDelay
        +bool batchReceive
        +bool batchAck
        +bool deadLetter
        +bool consumerLag
        +bool deliveryCount
    }
    class RedisTransport
    class RabbitMqTransport {
        +close() void
    }
    class InMemoryTransport {
        +newConsumer() InMemoryTransport
    }
    TransportInterface <|.. RedisTransport
    TransportInterface <|.. RabbitMqTransport
    TransportInterface <|.. InMemoryTransport
    SetupAwareInterface <|.. RedisTransport
    SetupAwareInterface <|.. RabbitMqTransport
    SetupAwareInterface <|.. InMemoryTransport
    MaintenanceAwareInterface <|.. RedisTransport
    MaintenanceAwareInterface <|.. InMemoryTransport
    StatsAwareInterface <|.. RedisTransport
    StatsAwareInterface <|.. RabbitMqTransport
    StatsAwareInterface <|.. InMemoryTransport
    BatchAckAwareInterface <|.. RedisTransport
    BatchAckAwareInterface <|.. RabbitMqTransport
    BatchAckAwareInterface <|.. InMemoryTransport
    DeadLetterAwareInterface <|.. RedisTransport
    DeadLetterAwareInterface <|.. RabbitMqTransport
    DeadLetterAwareInterface <|.. InMemoryTransport
    PurgeAwareInterface <|.. RedisTransport
    PurgeAwareInterface <|.. RabbitMqTransport
    PurgeAwareInterface <|.. InMemoryTransport
    BrowseAwareInterface <|.. RedisTransport
    BrowseAwareInterface <|.. RabbitMqTransport
    BrowseAwareInterface <|.. InMemoryTransport
    TransportInterface ..> TransportCapabilities
```

RabbitMQ does not implement `MaintenanceAwareInterface`: orphans come back natively when the channel closes.

### 3.3 Message model

```mermaid
classDiagram
    direction LR
    class Message {
        +MessageId id
        +string channel
        +string text
        +DateTimeImmutable createdAt
        +int attempts
        +DateTimeImmutable expiresAt
        +array headers
        +withIncrementedAttempts() Message
        +withAttempts(int attempts) Message
        +isExpired(DateTimeImmutable now) bool
    }
    class MessageId {
        +string value
        +fromString(string value)$ MessageId
        +equals(MessageId other) bool
    }
    class Delivery {
        +Message message
        +string receipt
        +DateTimeImmutable receivedAt
        +int deliveryCount
    }
    class Envelope {
        +Message message
        +int delayMs
    }
    class OutgoingMessage {
        +string channel
        +string text
        +array headers
        +int delayMs
        +int ttlMs
    }
    class QueuedMessage {
        +Message message
        +QueuedMessageState state
        +DateTimeImmutable availableAt
    }
    class QueuedMessageState {
        <<enumeration>>
        InFlight
        Ready
        Delayed
    }
    class DeadLetter {
        +Message message
        +FailureReason reason
    }
    class FailureReason {
        +FailureKind kind
        +MessageId originalId
        +DateTimeImmutable failedAt
        +string errorMessage
        +string errorClass
        +fromThrowable(FailureKind kind, MessageId id, DateTimeImmutable at, Throwable error)$ FailureReason
    }
    class FailureKind {
        <<enumeration>>
        HandlerFailed
        Unrecoverable
        Serialization
        UnknownChannel
        MaxDeliveriesExceeded
        Expired
    }
    class DecodedText {
        +TextKind kind
        +mixed value
        +string raw
        +isJson() bool
    }
    class TextDecoder {
        +decode(string text) DecodedText
    }
    class EnvelopeCodecInterface {
        <<interface>>
        +encode(Message message) array
        +decode(array fields) Message
    }
    Message --> MessageId
    Delivery --> Message
    Envelope --> Message
    QueuedMessage --> Message
    QueuedMessage --> QueuedMessageState
    DeadLetter --> Message
    DeadLetter --> FailureReason
    FailureReason --> FailureKind
    TextDecoder ..> DecodedText
    EnvelopeCodecInterface <|.. EnvelopeCodec
    EnvelopeCodec ..> Message
```

### 3.4 Producer and worker

```mermaid
classDiagram
    direction TB
    class ProducerInterface {
        <<interface>>
        +publish(string channel, string text, array headers, int delayMs, int ttlMs) MessageId
        +publishBatch(iterable messages) list~MessageId~
    }
    class Producer
    class ProducerMiddlewareInterface {
        <<interface>>
        +process(Message message, callable next) void
    }
    class WorkerInterface {
        <<interface>>
        +on(string channel, HandlerInterface or callable handler) static
        +run() WorkerExitReason
        +runOnce() int
        +stop() void
    }
    class Worker
    class MultiQueueWorker {
        +run() WorkerExitReason
        +runOnce() int
        +stop() void
    }
    class WorkerConfig {
        +int batchSize
        +int receiveTimeoutMs
        +int maxMessages
        +int maxRuntimeSeconds
        +int memoryLimitMb
        +bool stopWhenEmpty
        +int maintenanceIntervalMs
        +int gcEveryMessages
        +RetryStrategyInterface retryStrategy
        +bool retryAnyThrowable
        +UnknownChannelPolicy unknownChannelPolicy
        +ExpiredMessagePolicy expiredMessagePolicy
        +list middlewares
        +list listeners
    }
    class HandlerInterface {
        <<interface>>
        +handle(Message message, DecodedText text) void
    }
    class MiddlewareInterface {
        <<interface>>
        +process(Message message, callable next) void
    }
    class RetryStrategyInterface {
        <<interface>>
        +nextDelayMs(int attempt) int
    }
    class WorkerEventListenerInterface {
        <<interface>>
        +onEvent(WorkerEventInterface event) void
    }
    class SignalHandlerInterface {
        <<interface>>
        +onTerminate(Closure callback) void
        +reset() void
    }
    ProducerInterface <|.. Producer
    Producer o--> ProducerMiddlewareInterface
    ProducerMiddlewareInterface <|.. CorrelationIdMiddleware
    ProducerMiddlewareInterface <|.. TraceContextMiddleware
    Producer --> TransportInterface
    WorkerInterface <|.. Worker
    MultiQueueWorker o--> "1..*" WorkerInterface
    Worker --> TransportInterface
    Worker --> WorkerConfig
    Worker --> HandlerRegistry
    HandlerRegistry o--> HandlerInterface
    HandlerInterface <|.. CallableHandler
    WorkerConfig o--> MiddlewareInterface
    WorkerConfig o--> WorkerEventListenerInterface
    WorkerConfig --> RetryStrategyInterface
    RetryStrategyInterface <|.. ExponentialBackoff
    RetryStrategyInterface <|.. FixedDelay
    RetryStrategyInterface <|.. NoRetry
    MiddlewareInterface <|.. LoggingMiddleware
    MiddlewareInterface <|.. TimeoutMiddleware
    MiddlewareInterface <|.. IdempotencyMiddleware
    MiddlewareInterface <|.. CorrelationContextMiddleware
    IdempotencyMiddleware --> IdempotencyStoreInterface
    IdempotencyStoreInterface <|.. InMemoryIdempotencyStore
    IdempotencyStoreInterface <|.. RedisIdempotencyStore
    Worker --> SignalHandlerInterface
```

### 3.5 Redis adapter

```mermaid
classDiagram
    direction LR
    class RedisTransport {
        -RedisClientInterface client
        -RedisFeatures features
        -string consumer
    }
    class RedisClientInterface {
        <<interface>>
        +execute(list command) mixed
        +pipeline(list commands) list
    }
    class RedisFeatures {
        +IdleClaimStrategyInterface idleClaim
        +AckStrategyInterface ack
        +TrimStrategyInterface trim
        +LagStrategyInterface lag
        +LockReleaseStrategyInterface lockRelease
    }
    class FeatureSelector {
        +select(RedisCapabilities capabilities)$ RedisFeatures
    }
    class VersionDetector {
        +resolve(RedisVersionEnum override, bool verify) RedisVersionEnum
    }
    class RedisVersionEnum {
        <<enumeration>>
        GE60
        GE62
        GE70
        GE80
        GE82
        GE84
    }
    class RedisScripts {
        +ack()$ string
        +settle()$ string
        +requeueOrphans()$ string
        +promoteDelayed()$ string
        +requeueDeadLetters()$ string
        +purgeDeadLetters()$ string
        +purgeReady()$ string
        +purgeDelayed()$ string
        +releaseLock()$ string
    }
    RedisTransport --> RedisClientInterface
    RedisTransport --> RedisFeatures
    RedisTransport ..> RedisScripts
    RedisClientInterface <|.. PhpRedisClient
    RedisClientInterface <|.. PredisClient
    FeatureSelector ..> RedisFeatures
    VersionDetector ..> RedisVersionEnum
    RedisFeatures --> IdleClaimStrategyInterface
    RedisFeatures --> AckStrategyInterface
    RedisFeatures --> TrimStrategyInterface
    IdleClaimStrategyInterface <|.. PendingClaimStrategy
    IdleClaimStrategyInterface <|.. PendingIdleStrategy
    IdleClaimStrategyInterface <|.. ReadGroupClaimStrategy
    AckStrategyInterface <|.. AckThenDeleteStrategy
    AckStrategyInterface <|.. AckDelStrategy
    TrimStrategyInterface <|.. NoTrimStrategy
    TrimStrategyInterface <|.. MinIdTrimStrategy
    TrimStrategyInterface <|.. AckedTrimStrategy
```

### 3.6 RabbitMQ adapter

```mermaid
classDiagram
    direction LR
    class RabbitMqTransport {
        -AmqpConnectorInterface connector
        -Topology topology
        -RetryTiers retryTiers
        -int prefetch
        -list buffer
        +close() void
    }
    class AmqpConnectorInterface {
        <<interface>>
        +channel() AmqpChannelInterface
        +generation() int
        +reset() void
    }
    class AmqpChannelInterface {
        <<interface>>
        +declareExchange(string name, string type, array arguments) void
        +declareQueue(string name, array arguments, bool passive) array
        +bindQueue(string queue, string exchange, string routingKey) void
        +purgeQueue(string queue) int
        +prefetch(int count) void
        +consume(string queue, Closure onMessage) void
        +wait(int timeoutMs) bool
        +get(string queue) AmqpMessage
        +publish(AmqpMessage message, string exchange, string routingKey) void
        +confirm(int timeoutMs) list
        +ack(int deliveryTag) void
        +nack(int deliveryTag, bool requeue) void
    }
    class Topology {
        +string exchange
        +string queue
        +string deadLetterQueue
        +retryQueue(int tierMs) string
        +declare(AmqpChannelInterface channel) void
    }
    class RetryTiers {
        +tierFor(int delayMs) int
    }
    RabbitMqTransport --> AmqpConnectorInterface
    RabbitMqTransport --> Topology
    RabbitMqTransport --> RetryTiers
    AmqpConnectorInterface <|.. PhpAmqpLibConnector
    AmqpChannelInterface <|.. PhpAmqpLibChannel
    PhpAmqpLibConnector ..> PhpAmqpLibChannel : opens
```

### 3.7 Exceptions

```mermaid
classDiagram
    direction TB
    RuntimeException <|-- QueueException
    QueueException <|-- BackendException
    BackendException <|-- ConnectionException
    BackendException <|-- CommandFailedException
    BackendException <|-- VersionMismatchException
    BackendException <|-- BatchSendException
    QueueException <|-- UnsupportedStrategyException
    QueueException <|-- SerializationException
    QueueException <|-- UnrecoverableMessageException
    QueueException <|-- UnknownChannelException
    QueueException <|-- RetryableException
    class QueueException { <<abstract>> }
    class BackendException { <<abstract>> }
    class BatchSendException {
        +list sentIds
        +list failedIds
    }
    class RetryableException {
        +int delayMs
    }
```

---

## 4. Sequence diagrams

### 4.1 Creating a queue (Redis, version auto-detection)

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant F as QueueFactory
    participant B as RedisTransportBuilder
    participant V as VersionDetector
    participant R as Redis
    App->>F: create(StrategyDtoRedis, QueueConfig)
    F->>B: supports(dto)
    B-->>F: true
    F->>B: build(dto, config)
    B->>V: resolve(override, verifyVersion)
    V->>R: INFO server
    R-->>V: redis_version 8.4.7
    alt override above the server version
        V-->>B: VersionMismatchException
    else INFO denied by ACL and no override
        V-->>B: GE60 + warning log
    else
        V-->>B: GE84
    end
    B->>B: FeatureSelector.select(capabilities)
    B-->>F: RedisTransport
    F-->>App: Queue (facade)
```

### 4.2 Publishing a message

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant P as Producer
    participant MW as Producer middleware
    participant T as Transport
    participant BK as Broker
    App->>P: publish(channel, text, headers, delayMs, ttlMs)
    P->>P: check text size, new UUIDv7, createdAt, expiresAt
    P->>MW: process(message, next)
    MW->>MW: add correlation_id, causation_id, traceparent
    MW->>P: next(message)
    P->>P: check headers size
    P->>T: send(message, delayMs)
    alt Redis, no delay
        T->>BK: XADD stream * fields
    else Redis, delayed
        T->>BK: ZADD delayed dueAtMs member
    else RabbitMQ, no delay
        T->>BK: basic.publish exchange, routing key = channel
        BK-->>T: publisher confirm
    else RabbitMQ, delayed
        T->>BK: basic.publish wait queue of the tier, x-qb-due-at
        BK-->>T: publisher confirm
    end
    P-->>App: MessageId
```

### 4.3 Consuming: success

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant T as Transport
    participant MW as Middleware pipeline
    participant H as Handler
    loop until a stop condition
        W->>T: maintain() if due (Redis, InMemory)
        W->>T: receive(batchSize, receiveTimeoutMs)
        T-->>W: list of Delivery
        loop each delivery
            W->>W: deliveryCount above maxDeliveries? expired? handler for channel?
            W->>MW: process(message, next)
            MW->>H: handle(message, DecodedText)
            H-->>MW: return
            MW-->>W: return
            W->>W: queue the ack
        end
        W->>T: ackBatch(deliveries)
    end
```

### 4.4 Consuming: failure, retry and dead letter

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant H as Handler
    participant S as RetryStrategy
    participant T as Transport
    W->>H: handle(message)
    H-->>W: throws RetryableException
    W->>W: emit MessageFailed, attempt = attempts + 1
    alt attempt below maxAttempts
        W->>S: nextDelayMs(attempt)
        S-->>W: 2000
        W->>T: retry(delivery, 2000)
        Note over T: Redis: one Lua script writes the copy (attempts + 1) and acks the original<br/>RabbitMQ: publish copy, wait for confirm, then ack
        W->>W: emit MessageRetried
    else attempts exhausted, or UnrecoverableMessageException
        W->>T: reject(delivery, FailureReason)
        Note over T: copy in the DLQ with the reason, then ack (same guarantee as retry)
        W->>W: emit MessageDeadLettered
    end
```

### 4.5 Worker crash and orphan recovery on Redis

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1
    participant R as Redis
    participant W2 as Worker 2
    W1->>R: XREADGROUP GROUP queue-bridge worker-1 ... STREAMS stream >
    R-->>W1: entry 1-0 (now in the PEL of worker-1)
    Note over W1: crash (segfault, OOM, kill -9): no ack
    loop every maintenanceIntervalMs
        W2->>R: maintain(): promote delayed, find idle pending entries
    end
    Note over R: entry idle for more than claimIdleMs
    W2->>R: EVALSHA requeueOrphans (re-checks the idle time)
    R->>R: XADD copy with deliveries = total, ack + delete 1-0
    W2->>R: XREADGROUP ... >
    R-->>W2: the copy, deliveryCount = previous deliveries + 1
    Note over W2,R: Redis 8.4: XREADGROUP ... CLAIM claimIdleMs returns the idle entry directly
```

### 4.6 Worker crash on RabbitMQ

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1
    participant Q as Quorum queue
    participant W2 as Worker 2
    W1->>Q: basic.consume (prefetch N)
    Q-->>W1: basic.deliver tag 7
    Note over W1: crash: TCP connection closed
    Q->>Q: unacked messages requeued, x-delivery-count + 1
    Q-->>W2: basic.deliver (x-delivery-count = 1)
    W2->>W2: deliveryCount = 2, compared with maxDeliveries
    Note over Q: beyond x-delivery-limit the broker itself dead-letters the message to the DLQ
```

### 4.7 Graceful shutdown

```mermaid
sequenceDiagram
    autonumber
    participant OS as Supervisor (systemd, k8s)
    participant SH as SignalHandler
    participant W as Worker
    participant T as Transport
    OS->>SH: SIGTERM
    SH->>W: stop()
    Note over W: the current message is completed and acknowledged
    loop the rest of the batch already received
        W->>T: release(delivery)
        Note over T: back in the queue, attempts unchanged
    end
    W->>SH: reset()
    W-->>OS: run() returns StopRequested, exit code 0
```

### 4.8 Idempotency middleware

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant I as IdempotencyMiddleware
    participant S as IdempotencyStore
    participant H as Handler
    W->>I: process(message, next)
    I->>S: acquire(id, lockTtlMs)
    alt already completed
        S-->>I: Completed
        I-->>W: return (the worker acknowledges the duplicate)
    else being processed by another worker
        S-->>I: InProgress(lockRemainingMs)
        I-->>W: RetryableException(delayMs = lockRemainingMs)
    else lock acquired
        S-->>I: Acquired(LockToken)
        I->>H: next(message)
        alt handler succeeds
            I->>S: complete(token, doneTtlMs)
        else handler throws
            I->>S: release(token) (only if the token matches)
            I-->>W: rethrow
        end
    end
```

### 4.9 Purging a queue on Redis

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant T as RedisTransport
    participant R as Redis
    App->>T: purge()
    T->>R: EVALSHA purgeDelayed (ZCARD + DEL delayed)
    T->>R: XREVRANGE stream + - COUNT 1
    R-->>T: newest id N
    loop pages of 500 up to N
        T->>R: EVALSHA purgeReady(group, start, N, 500)
        R->>R: XRANGE, XPENDING on the range, XDEL entries not pending
        R-->>T: deleted, scanned, last id
    end
    T-->>App: delayed + deleted
```

### 4.10 Listing a queue on RabbitMQ

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant T as RabbitMqTransport
    participant Q as Main queue
    App->>T: browse(limit)
    T->>T: messages already prefetched by this consumer (ready)
    loop until limit or queue empty
        T->>Q: basic.get
        Q-->>T: message (unacked)
    end
    T->>Q: basic.nack requeue for every message taken
    T-->>App: list of QueuedMessage (Ready, or Delayed if x-qb-due-at is in the future)
```

### 4.11 Several queues in one process

```mermaid
sequenceDiagram
    autonumber
    participant M as MultiQueueWorker
    participant A as Worker orders
    participant B as Worker emails
    loop until a stop condition of the whole set
        M->>A: runOnce()
        A-->>M: processed
        M->>B: runOnce()
        B-->>M: processed
    end
    Note over M,B: SIGTERM: M.stop() calls stop() on every inner worker, which releases the messages already received
```

---

## 5. Activity diagrams

Mermaid has no dedicated UML activity diagram, so activities are drawn as flowcharts with the UML conventions: a rounded
start/end node, actions as rectangles, decisions as diamonds.

### 5.1 Worker main loop (`Worker::run()`)

```mermaid
flowchart TD
    S(["run()"]) --> R["register SIGTERM/SIGINT handler"]
    R --> C{"stop requested?"}
    C -- yes --> X1["reason = StopRequested"]
    C -- no --> L{"maxMessages, maxRuntimeSeconds or memoryLimitMb reached?"}
    L -- yes --> X2["reason = limit reached"]
    L -- no --> M{"maintenance due? (maintenanceIntervalMs)"}
    M -- yes --> MT["transport.maintain()"]
    M -- no --> RC
    MT --> RC["receive(batch, receiveTimeoutMs)"]
    RC --> E{"received nothing and stopWhenEmpty?"}
    E -- yes --> X3["reason = QueueEmpty"]
    E -- no --> FE["for each delivery"]
    FE --> SR{"stop requested meanwhile?"}
    SR -- yes --> RL["release(delivery)"]
    SR -- no --> AG{"received too long ago to finish before claimIdleMs? (Redis, InMemory)"}
    AG -- yes --> RL
    AG -- no --> PR["process(delivery): see 5.2"]
    RL --> NX{"more deliveries?"}
    PR --> NX
    NX -- yes --> FE
    NX -- no --> FA["flush acks (ackBatch), periodic gc_collect_cycles"]
    FA --> C
    X1 --> END
    X2 --> END
    X3 --> END
    END["reset signal handler, emit WorkerStopped"] --> F(["return WorkerExitReason"])
```

### 5.2 Processing one delivery

```mermaid
flowchart TD
    S(["delivery"]) --> EV["emit MessageReceived"]
    EV --> D{"deliveryCount above maxDeliveries?"}
    D -- yes --> DLQ1["reject: MaxDeliveriesExceeded"]
    D -- no --> EXP{"expired?"}
    EXP -- yes --> EP{"expiredMessagePolicy"}
    EP -- Discard --> ACK1["ack + MessageDiscarded"]
    EP -- DeadLetter --> DLQ2["reject: Expired"]
    EXP -- no --> HC{"handler for the channel, or '*'?"}
    HC -- no --> UP{"unknownChannelPolicy"}
    UP -- Discard --> ACK2["ack + MessageDiscarded"]
    UP -- DeadLetter --> DLQ3["reject: UnknownChannel"]
    HC -- yes --> RUN["middleware pipeline, then handler(message, DecodedText)"]
    RUN --> OK{"exception?"}
    OK -- no --> ACK3["ack + MessageHandled"]
    OK -- yes --> K{"exception type"}
    K -- "UnrecoverableMessageException / SerializationException" --> DLQ4["reject: Unrecoverable / Serialization"]
    K -- "RetryableException, or any Throwable with retryAnyThrowable" --> A{"attempts + 1 below maxAttempts and a delay is available?"}
    K -- "other Throwable" --> DLQ5["reject: HandlerFailed"]
    A -- yes --> RT["retry(delivery, delay) + MessageRetried"]
    A -- no --> DLQ5
    DLQ1 --> F(["done"])
    DLQ2 --> F
    DLQ3 --> F
    DLQ4 --> F
    DLQ5 --> F
    ACK1 --> F
    ACK2 --> F
    ACK3 --> F
    RT --> F
```

### 5.3 Delayed delivery on RabbitMQ (retry tiers)

```mermaid
flowchart TD
    S(["send or retry with delayMs > 0"]) --> T["dueAt = now + delayMs"]
    T --> P["tier = largest tier not above the remaining delay (at least the smallest)"]
    P --> W["publish to wait queue rq.queue.retry.tier with x-qb-due-at"]
    W --> TTL["the message waits the tier TTL"]
    TTL --> DL["dead-lettered (at-least-once) to the main queue"]
    DL --> RCV["received by a consumer"]
    RCV --> Q{"x-qb-due-at in the future?"}
    Q -- yes --> P
    Q -- no --> H(["handed to the worker"])
```

### 5.4 Redis level selection

```mermaid
flowchart TD
    S(["RedisTransportBuilder.build()"]) --> O{"version override in the DTO?"}
    O -- no --> I{"INFO server allowed?"}
    I -- no --> B["GE60 baseline + warning"]
    I -- yes --> D["level = highest level not above redis_version"]
    O -- yes --> VV{"verifyVersion?"}
    VV -- no --> U["use the override"]
    VV -- yes --> I2{"INFO allowed?"}
    I2 -- no --> U
    I2 -- yes --> CMP{"override above the server?"}
    CMP -- yes --> E(["VersionMismatchException"])
    CMP -- no --> U
    B --> SEL
    D --> SEL
    U --> SEL["FeatureSelector: one strategy per capability"]
    SEL --> F(["RedisTransport"])
```

### 5.5 Purging a queue

```mermaid
flowchart TD
    S(["purge()"]) --> BK{"backend"}
    BK -- Redis --> R1["delete the delayed sorted set"]
    R1 --> R2["read the newest stream id N"]
    R2 --> R3{"stream empty?"}
    R3 -- yes --> F
    R3 -- no --> R4["page of 500 entries from the cursor up to N"]
    R4 --> R5["delete the entries not in the PEL (in-flight entries stay)"]
    R5 --> R6{"page full?"}
    R6 -- yes --> R4
    R6 -- no --> F
    BK -- RabbitMQ --> Q1["queue.purge main queue and every wait queue"]
    Q1 --> Q2{"this transport is consuming?"}
    Q2 -- yes --> Q3["read the prefetched messages and ack them"]
    Q2 -- no --> F
    Q3 --> F
    BK -- InMemory --> M1["clear ready and delayed lists"]
    M1 --> F(["return the number of removed messages"])
```

---

## 6. Specifications

### 6.1 Message

| Field | Type | Rules |
|---|---|---|
| `id` | `MessageId` | UUIDv7 generated by the producer (monotonic within the same millisecond), kept across retries and DLQ requeues |
| `channel` | `string` | 1–255 characters, `^[A-Za-z0-9._:-]+$` |
| `text` | `string` | valid UTF-8, at most `QueueConfig::$maxTextBytes` bytes (default 1 MiB, measured with `strlen`) |
| `attempts` | `int` | failed handler runs so far, starts at 0, `+1` at every retry, reset to 0 by a DLQ requeue |
| `createdAt` | `DateTimeImmutable` | UTC, from the PSR-20 clock |
| `expiresAt` | `?DateTimeImmutable` | `createdAt + ttlMs`; null = never expires |
| `headers` | `array<string,string>` | metadata (`correlation_id`, `causation_id`, `traceparent`...), total size at most `maxHeadersBytes` (default 64 KiB) |

`Delivery` adds the backend `receipt` (opaque to the core), `receivedAt` and `deliveryCount` (deliveries of the current copy,
crashes included; a retry creates a new copy and restarts from 1).

### 6.2 Text decoding

1. `json_validate()` true → `json_decode(..., associative: true, JSON_BIGINT_AS_STRING)` → `DecodedText(kind: Json)`;
   scalars (`123`, `true`, `"x"`, `null`) are JSON too.
2. Otherwise, including the empty string and JSON deeper than the maximum depth (default 512) → `DecodedText(kind: Plain)`
   with the raw text.
3. It never throws.

### 6.3 Wire format (version 1)

`EnvelopeCodec` turns a `Message` into a flat `array<string,string>`: `v` (`"1"`), `id`, `channel`, `text`, `attempts`,
`created_at`, `expires_at`, `headers` (JSON). It reads every supported version and always writes the latest; an unknown
version or invalid fields raise `SerializationException` and the message goes to the DLQ without reaching a handler. A new
format will be a new version, with the previous decoder kept for at least one major version, so that workers of different
versions can run together during a rolling deploy.

- **Redis**: the map is the stream entry. Extra fields: `deliveries` (deliveries of previous copies) and, in the DLQ,
  `failure_kind`, `failure_error_class`, `failure_message`, `failure_at`, `failure_original_id`.
- **RabbitMQ**: `text` is the body, the other fields are `x-qb-<field>` headers, `message_id` is the ID, the routing key is
  the channel. Extra headers: `x-qb-due-at`, `x-qb-failure-*`.

### 6.4 Configuration

`QueueConfig` (same for every backend):

| Option | Default | Rule | Meaning |
|---|---|---|---|
| `name` | `default` | `^[A-Za-z0-9._:-]+$` | queue name |
| `prefix` | `rq` | same pattern | prefix of Redis keys and RabbitMQ resources |
| `maxTextBytes` | 1,048,576 | >= 1 | maximum `text` size in bytes |
| `maxHeadersBytes` | 65,536 | >= 0 | maximum total size of headers |
| `maxAttempts` | 3 | >= 1 | failed attempts before the DLQ |
| `maxDeliveries` | 10 | >= `maxAttempts + 1` | deliveries without ack before the DLQ (poison protection) |
| `claimIdleMs` | 300,000 | >= 1000 | idle time after which an in-flight message is an orphan (Redis, InMemory) |
| `dlqMaxLength` | 100,000 | >= 1 | DLQ retention (Redis `MAXLEN ~`, InMemory) |

`WorkerConfig`:

| Option | Default | Meaning |
|---|---|---|
| `batchSize` | 10 | messages per `receive()` |
| `receiveTimeoutMs` | 1000 | blocking wait when the queue is empty |
| `maxMessages` | null | exit after this many messages (`MessageLimitReached`) |
| `maxRuntimeSeconds` | null | exit after this time (`TimeLimitReached`) |
| `memoryLimitMb` | null | exit above this memory usage (`MemoryLimitReached`) |
| `stopWhenEmpty` | false | exit at the first empty receive (`QueueEmpty`) |
| `maintenanceIntervalMs` | 5000 | how often `maintain()` runs (Redis, InMemory) |
| `gcEveryMessages` | 1000 | `gc_collect_cycles()` every N messages (0 = never) |
| `retryStrategy` | `ExponentialBackoff` (1 s, ×2, max 300 s, jitter 10%) | delay of the next attempt |
| `retryAnyThrowable` | false | retry any `Throwable`, not only `RetryableException` |
| `unknownChannelPolicy` | `DeadLetter` | `DeadLetter` or `Discard` |
| `expiredMessagePolicy` | `Discard` | `Discard` or `DeadLetter` |
| `middlewares` | `[]` | worker middleware, outermost first |
| `listeners` | `[]` | `WorkerEventListenerInterface` (a failing listener is logged, never stops the worker) |

Validated when the worker is created: `claimIdleMs` greater than the `TimeoutMiddleware` timeout.

### 6.5 Worker events and exit reasons

Events: `MessageReceived`, `MessageHandled` (with the duration), `MessageFailed`, `MessageRetried`, `MessageDeadLettered`,
`MessageDiscarded`, `MessageReleased`, `WorkerStopped`.

`WorkerExitReason`: `StopRequested`, `MessageLimitReached`, `TimeLimitReached`, `MemoryLimitReached`, `QueueEmpty`.
Backend exceptions are not caught: they leave `run()` and the process exits with a non-zero code, so that the supervisor
restarts it. Handler exceptions never stop the worker.

### 6.6 Transport contract (all backends)

- `send`/`sendBatch`: a partially failed batch throws `BatchSendException` with the IDs sent and the IDs not sent.
- `receive($max, $timeoutMs)`: at most `$max` messages in arrival order, an empty list after the timeout.
- `ack`, `retry`, `reject`, `release` on a delivery no longer owned (already acknowledged, or recovered by another consumer)
  have no effect.
- `retry` and `reject`: the new copy is written before the original is acknowledged: never lost, at most duplicated.
- `release`: the message is available again at once, `attempts` unchanged, `deliveryCount` not reset.
- DLQ requeue: `attempts = 0`, same ID.
- `purge`: removes ready and delayed messages present at the call; in-flight messages, the DLQ and the backend resources stay.
- `browse`: does not consume (IDs and `attempts` unchanged); in flight, then ready in delivery order, then delayed by due
  time; undecodable entries are skipped.

These rules are checked by `TransportContractTestCase`, run on InMemory (unit suite) and on real Redis 6.0 → 8.4 and
RabbitMQ 3.13 / 4.x servers (integration suite).

### 6.7 Capability matrix

| Capability | Redis | RabbitMQ | InMemory |
|---|---|---|---|
| Orphan recovery | explicit (`maintain()`, or `XREADGROUP CLAIM` on 8.4) | native (channel close) | explicit (`maintain()`) |
| Delays | exact (sorted set) | wait-queue tiers | exact |
| Delivery count | yes (PEL + `deliveries` field) | yes (`x-delivery-count`) | yes |
| Batch ack | yes (one Lua script) | yes (one `basic.ack` per tag) | yes |
| Stats | ready, in flight, delayed, DLQ, lag (>= 7.0), consumers | ready, in flight of this consumer, delayed, DLQ, consumers | ready, in flight, delayed, DLQ, lag |
| Purge | delayed + stream entries not pending | `queue.purge` main + wait queues, prefetched messages acked | ready + delayed |
| Browse | in flight, ready, delayed (read-only) | ready only (`basic.get` + requeue) | in flight, ready, delayed |

### 6.8 Redis layout and versions

| Key (`prefix = rq`, queue `messages`) | Type | Purpose |
|---|---|---|
| `rq:{messages}:stream` | Stream | main queue, consumer group `queue-bridge` |
| `rq:{messages}:delayed` | Sorted set | delayed messages, score = due time in ms, member = JSON of the fields |
| `rq:{messages}:dlq` | Stream | dead letter queue (`MAXLEN ~ dlqMaxLength`) |
| `rq:{messages}:idem:{id}` | String + TTL | idempotency (`RedisIdempotencyStore`) |

Hash tags keep all the keys of a queue in one Redis Cluster slot (multi-key Lua scripts). Never `MAXLEN` on the main stream:
trimming would delete unread or pending messages; the stream empties itself through ack + delete.

| Level | Used commands | Fallback below |
|---|---|---|
| GE60 | Streams, consumer group, extended `XPENDING`, `EVALSHA` | — |
| GE62 | `XPENDING ... IDLE`, `XTRIM MINID` | client-side idle filter, no safety trim |
| GE70 | `XINFO GROUPS` lag | lag = null |
| GE80 | baseline (no new stream commands) | as GE70 |
| GE82 | `XACKDEL`, `XTRIM ... ACKED` | `XACK` + `XDEL`, trim with computed `MINID` |
| GE84 | `XREADGROUP ... CLAIM`, `DELEX ... IFEQ` | maintenance requeue script, Lua lock release |

### 6.9 RabbitMQ topology

| Resource | Type | Arguments |
|---|---|---|
| `rq.messages` (exchange) | topic, durable | routing key = channel |
| `rq.messages` (queue) | quorum, durable, bound with `#` | `x-delivery-limit = maxDeliveries`, `x-dead-letter-strategy = at-least-once`, `x-overflow = reject-publish`, dead letter to `rq.messages.dlq` |
| `rq.messages.retry.<ms>` | quorum, one per tier | `x-message-ttl = <ms>`, at-least-once dead letter to the main queue |
| `rq.messages.dlq` | classic, durable | inspection with `basic.get` + `nack` |

Messages are persistent and `mandatory`; publisher confirms are on by default. Receipts are `generation:delivery-tag`, so
after the channel is reopened old receipts have no effect.

### 6.10 Exceptions

| Exception | Raised when | Worker reaction |
|---|---|---|
| `ConnectionException` | backend unreachable (after the client's reconnection attempts) | leaves `run()` |
| `CommandFailedException` | a command was rejected or the reply is malformed | leaves `run()` |
| `VersionMismatchException` | Redis override above the server version | at `create()` |
| `BatchSendException` | partial failure of `sendBatch()` | — (producer side) |
| `UnsupportedStrategyException` | no builder accepts the DTO | at `create()` |
| `SerializationException` | invalid wire format | DLQ (`Serialization`) |
| `UnrecoverableMessageException` | thrown by a handler | DLQ (`Unrecoverable`), no retry |
| `RetryableException` | thrown by a handler, optional `delayMs` | retry |
| `UnknownChannelException` | no handler for the channel | DLQ or discard, by policy |

### 6.11 CLI and DSN

`bin/queue-worker` commands: `work --bootstrap=<file.php>`, `stats`, `ping`, `queue:list`, `queue:purge --force`,
`dlq:list`, `dlq:requeue [--id]`, `dlq:purge --force`. Exit codes: `0` success, `1` backend error, `2` usage error.

DSN: `redis[s]://[user:pass@]host[:port][/db]?client=phpredis|predis&version=&verify_version=&consumer=&timeout=`,
`amqp[s]://user:pass@host[:port][/vhost]?prefetch=&confirms=&tiers=ms,ms&heartbeat=&timeout=`, `memory://`.

---

## 7. Concurrency

### 7.1 Model

A worker is a **single PHP process with a single thread**: it receives a batch and processes its messages one after the
other. It never forks and never uses threads. **Parallelism comes from running several processes** (on one or many
machines) on the same queue; the broker distributes the messages among them (competing consumers). This keeps the handlers
simple (no shared memory, no locks inside the process) and makes the system scale horizontally.

### 7.2 What guarantees that two workers never get the same message

- **Redis**: `XREADGROUP ... >` is atomic on the server: each entry is delivered to exactly one consumer of the group and
  recorded in its pending list. Every state change of a delivery (ack, retry, reject, release) is a **Lua script** that first
  checks with `XPENDING` that the entry still belongs to the calling consumer, then writes and acknowledges in one atomic step.
  A worker that was too slow and lost its message to orphan recovery cannot acknowledge or retry the new owner's copy.
- **RabbitMQ**: the broker delivers each message to one consumer. Delivery tags are per channel; receipts carry the channel
  generation, so a stale receipt after a reconnection is ignored instead of acknowledging the wrong message.
- **InMemory**: one process, shared state in `InMemoryBroker`; `newConsumer()` creates a second competing consumer for tests.

The contract test `testConcurrentConsumersNeverReceiveTheSameMessage` verifies this on every backend.

### 7.3 Orphan recovery without double processing

- Several workers may run maintenance at the same time: the requeue script **re-checks the idle time inside Lua**, so an
  orphan is copied back exactly once.
- `claimIdleMs` must exceed the longest handler duration (checked against `TimeoutMiddleware`): otherwise a message still being
  processed would be considered an orphan and processed again in parallel.
- On RabbitMQ there is no idle threshold: a message comes back only when its consumer's channel or connection closes. Keep
  handlers below the broker `consumer_timeout` (default 30 minutes) and use heartbeats (`PCNTLHeartbeatSender` with
  `ext-pcntl`), otherwise the broker closes the channel and the message is redelivered.

### 7.4 Duplicates are possible: idempotency

At-least-once means a message can be processed twice: crash after the side effect and before the ack, an orphan recovered
while the original worker was only slow, a publish confirmed by the broker after a network timeout. `IdempotencyMiddleware`
serializes concurrent duplicates with a **two-phase lock** in a shared store:

1. `acquire(id, lockTtlMs)`: `SET NX PX` on Redis, in a Lua script that tells the three outcomes apart atomically:
   - `Acquired`: run the handler;
   - `Completed`: acknowledge the duplicate without running the handler;
   - `InProgress`: another worker holds the lock. The message is **not** acknowledged (that worker may still fail or die,
     and the message would be lost): `RetryableException` with a delay equal to the remaining lock time. The retry uses
     one attempt of `maxAttempts`;
2. handler;
3. `complete(token, doneTtlMs)` marks it done for a long TTL; on error `release(token)` frees it only if the token still
   matches (compare-and-delete: `DELEX ... IFEQ` on Redis 8.4, a Lua script before).

The store is independent of the transport: with RabbitMQ pass a `RedisIdempotencyStore` or your own implementation.

### 7.5 Producers

Producers are stateless apart from the ID generator, and any number of processes can publish concurrently. UUIDv7 IDs are
unique across processes (random part) and monotonic within one process. A batch is not a transaction: if part of it fails,
`BatchSendException` lists what was sent, so the caller can resend only the rest.

### 7.6 Maintenance, purge and browse under load

- Delayed promotion is one Lua script per call (up to 100 messages), so two workers promoting at the same time never duplicate
  a delayed message.
- `purge()` on Redis works in pages of 500 entries up to the newest ID read at the start: it does not block Redis on large
  backlogs, and it does not chase messages published while it runs. Entries in the pending list are skipped, so messages
  being processed are never deleted under a worker's feet.
- `browse()` is a snapshot: with active workers, states change while you read. On RabbitMQ, `browse()` takes messages with
  `basic.get`, so for a short time they are invisible to the other consumers; they are given back with `nack` + requeue.

### 7.7 What is not handled

- **Ordering**: not guaranteed (retries, delays, parallel consumers).
- **Per-key serialization** (all messages of the same customer in sequence): not provided; use one queue per partition, or
  locks in the handler.
- **Exactly-once**: not provided; use idempotent handlers or `IdempotencyMiddleware`.
- **Fan-out**: one consumer group / one queue per `QueueConfig`.

---

## 8. Running the worker on a server

### 8.1 Principles

- The worker is a **long-running process**, not a cron job per message: it keeps the connection open and waits on the broker
  (`receiveTimeoutMs`), so latency is milliseconds and there is no start-up cost per message.
- Run it under a **process supervisor** (systemd, Supervisor, Kubernetes, Docker restart policy) that restarts it when it exits.
- Make it **exit on purpose regularly** with `--max-runtime` and `--memory-limit`: PHP processes accumulate memory and
  stale state; a clean exit followed by a restart is the simplest remedy. Exits are graceful.
- Scale by running **N identical processes**; each one is a competing consumer.
- Stop it with **SIGTERM** and give it more time than the slowest handler before SIGKILL: the current message is completed,
  the messages already received are given back with `release()`.
- `ext-pcntl` is needed for signals and for `TimeoutMiddleware`; `QueueBridge::factory()` enables signal handling when the
  extension is loaded.

### 8.2 The bootstrap file

The CLI loads a PHP file that returns a closure registering the handlers:

```php
<?php // config/queue-handlers.php

declare(strict_types=1);

use QueueBridge\Message\Message;
use QueueBridge\Text\DecodedText;
use QueueBridge\Worker\WorkerInterface;

require __DIR__.'/../vendor/autoload.php';
$container = require __DIR__.'/container.php';

return static function (WorkerInterface $worker) use ($container): void {
    $worker->on('order.created', static function (Message $message, DecodedText $text) use ($container): void {
        $container->get(OrderService::class)->onCreated($text->value);
    });
};
```

```
vendor/bin/queue-worker work --dsn="$QUEUE_DSN" --queue=orders --bootstrap=config/queue-handlers.php \
    --batch-size=20 --max-runtime=3600 --memory-limit=256
```

The same can be done in PHP with `$queue->worker(...)->on(...)->run()` (see `examples/08-worker-in-code.php`), for example
inside a framework console command.

### 8.3 systemd

A template unit runs N instances (`queue-worker@1`, `queue-worker@2`, ...). The Redis consumer name should be stable and
unique per instance, so that its pending list survives restarts:

```ini
# /etc/systemd/system/queue-worker@.service
[Unit]
Description=queue-bridge worker %i
After=network-online.target

[Service]
User=www-data
WorkingDirectory=/var/www/app
Environment=QUEUE_DSN=redis://127.0.0.1:6379/0?consumer=%H-%i
ExecStart=/usr/bin/php vendor/bin/queue-worker work --queue=orders --bootstrap=config/queue-handlers.php --max-runtime=3600 --memory-limit=256
Restart=always
RestartSec=1
KillSignal=SIGTERM
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

```
systemctl enable --now queue-worker@1 queue-worker@2 queue-worker@3
```

A ready-made copy is in [examples/deploy/queue-worker.service](examples/deploy/queue-worker.service).

### 8.4 Supervisor

```ini
[program:queue-worker]
command=php /var/www/app/vendor/bin/queue-worker work --queue=orders --bootstrap=/var/www/app/config/queue-handlers.php --max-runtime=3600 --memory-limit=256
environment=QUEUE_DSN="amqp://app:secret@rabbitmq:5672?prefetch=20"
process_name=%(program_name)s_%(process_num)02d
numprocs=4
autostart=true
autorestart=true
stopsignal=TERM
stopwaitsecs=60
```

See [examples/deploy/supervisord.conf](examples/deploy/supervisord.conf).

### 8.5 Kubernetes

One container runs one worker process; replicas give the parallelism. Use the pod name as the Redis consumer name, and a
termination grace period longer than the slowest handler.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-worker
spec:
  replicas: 4
  selector:
    matchLabels: { app: orders-worker }
  template:
    metadata:
      labels: { app: orders-worker }
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: worker
          image: registry.example.com/app:1.0.0
          command: ["php", "vendor/bin/queue-worker", "work", "--queue=orders",
                    "--bootstrap=config/queue-handlers.php", "--max-runtime=3600", "--memory-limit=256"]
          env:
            - name: POD_NAME
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
            - name: QUEUE_DSN
              value: "redis://redis:6379/0?consumer=$(POD_NAME)"
          readinessProbe:
            exec:
              command: ["php", "vendor/bin/queue-worker", "ping", "--queue=orders"]
            periodSeconds: 30
          resources:
            limits: { memory: "384Mi" }
```

The container restarts after `--max-runtime` (restart policy `Always`). Keep the container memory limit above
`--memory-limit` so that the worker exits by itself before the kernel kills it.

### 8.6 Cron (only for low-volume or batch queues)

When a permanent process is not possible (shared hosting), run the worker periodically and let it exit when the queue is empty,
with a lock so that runs do not overlap:

```
* * * * * flock -n /tmp/queue-orders.lock php /var/www/app/vendor/bin/queue-worker work --queue=orders \
    --bootstrap=/var/www/app/config/queue-handlers.php --stop-when-empty --max-runtime=55
```

Latency is up to one minute, and orphan recovery and delayed promotion happen only while a worker runs.

### 8.7 How many workers, and how to size them

- Start with **one process per CPU core** for CPU-bound handlers, more for I/O-bound handlers (HTTP calls, e-mail), and
  measure: if `stats()->ready` grows steadily, add processes; if workers are often idle, remove them.
- `batchSize` 10–50 is a good default; on RabbitMQ keep `prefetch` close to `batchSize`.
- Several small queues with low traffic can share one process with `MultiQueueWorker` (inner workers with a short
  `receiveTimeoutMs`, e.g. 100 ms).
- Slow handlers: raise `claimIdleMs` above the longest duration and protect them with `TimeoutMiddleware`.

### 8.8 Operations checklist

| Item | Recommendation |
|---|---|
| Redis | AOF on (`appendonly yes`, `appendfsync everysec`), `maxmemory-policy noeviction`, a stable `consumer` name per process |
| RabbitMQ | quorum queues (created by `setup()`), heartbeats, handlers below `consumer_timeout` |
| Setup | `$queue->setup()` (or the CLI `work`, which calls it) at deploy time; it is idempotent |
| Health | readiness: `queue-worker ping`; alert on `stats` (`ready` growing, `deadLetters` > 0, `lag`) |
| Logs | PSR-3 logger; `-v`/`-vv` on the CLI; `LoggingMiddleware` for per-message logs |
| Metrics | a `WorkerEventListenerInterface` that counts handled/failed/retried/dead-lettered messages and durations |
| DLQ | review regularly: `queue-worker dlq:list`, fix, then `dlq:requeue` |
| Deploy | rolling restarts are safe: SIGTERM releases in-flight work and the wire format is versioned |
| Emergencies | `queue-worker queue:purge --force` empties ready and delayed messages, `dlq:purge --force` the DLQ |
