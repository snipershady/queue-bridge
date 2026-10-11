# queue-bridge: documentazione di progetto

[English](DOC.md) | **Italiano**

Questo documento spiega come funziona queue-bridge: cos'è una coda di messaggi e come la libreria traduce ogni concetto in
codice, il modello UML (diagrammi delle classi, di sequenza e di attività in Mermaid), le specifiche implementate, come
viene gestita la concorrenza e come eseguire i worker su un server. Per l'installazione e una panoramica veloce vedi il
[README](README_IT.md).

## Indice

1. [Code di messaggi](#1-code-di-messaggi)
2. [Architettura](#2-architettura)
3. [Diagrammi delle classi](#3-diagrammi-delle-classi)
4. [Diagrammi di sequenza](#4-diagrammi-di-sequenza)
5. [Diagrammi di attività](#5-diagrammi-di-attività)
6. [Specifiche](#6-specifiche)
7. [Concorrenza](#7-concorrenza)
8. [Eseguire il worker su un server](#8-eseguire-il-worker-su-un-server)

---

## 1. Code di messaggi

### 1.1 A cosa serve una coda

Una coda di messaggi è un buffer tra il codice che **produce** lavoro e il codice che lo **esegue**. Il producer scrive un
messaggio e ritorna subito; uno o più consumer leggono i messaggi più tardi, con il proprio ritmo, anche su altre macchine.
Questo dà tre proprietà che una chiamata diretta non può dare:

- **Disaccoppiamento**: il producer non sa chi elabora il messaggio, né quando, né in quale processo.
- **Livellamento del carico**: un picco di 10.000 richieste diventa un arretrato che i worker smaltiscono alla velocità che
  riescono a sostenere, invece di 10.000 chiamate concorrenti che sovraccaricano il database.
- **Resilienza**: se il consumer è fermo, i messaggi aspettano in coda; non si perde nulla e chi chiama non deve ritentare.

Caso tipico: una richiesta HTTP che deve inviare un'e-mail risponde subito e lascia l'e-mail a un worker.

```php
// Nella richiesta web: pubblica e ritorna.
$queue->producer()->publish('mail.welcome', json_encode(['user' => 42], JSON_THROW_ON_ERROR));

// In un processo separato, sempre attivo: consuma.
$queue->worker()->on('mail.welcome', static function (Message $message, DecodedText $text): void {
    $mailer->sendWelcome($text->value['user']);
})->run();
```

### 1.2 Producer, broker, consumer, messaggio

| Concetto | Significato | In queue-bridge |
|---|---|---|
| **Messaggio** | un'unità di lavoro: payload più metadati | `Message`: `id` UUIDv7, `channel`, `text`, `attempts`, `createdAt`, `expiresAt`, `headers` |
| **Producer** | scrive i messaggi | `ProducerInterface::publish()` / `publishBatch()` |
| **Broker** | conserva i messaggi in modo durevole e li consegna | Redis (Streams) o RabbitMQ; `InMemory` per i test |
| **Consumer** | legge ed elabora i messaggi | `Worker` (`WorkerInterface`) o un tuo ciclo su `TransportInterface::receive()` |
| **Coda** | un flusso di messaggi con un nome sul broker | `QueueConfig::$name` (+ `$prefix`) |
| **Canale / routing key** | un'etichetta che sceglie l'handler | `Message::$channel`, `WorkerInterface::on($channel, $handler)` |

Il **canale** non è una coda separata: tutti i messaggi di una `QueueConfig` viaggiano nella stessa coda e il worker sceglie
l'handler in base al canale (`*` è il fallback). Usa code diverse quando ti servono scaling o priorità separati.

```php
$queue = QueueBridge::factory()->create(StrategyDtoRedis::fromPhpRedis($redis), new QueueConfig(name: 'orders'));

$queue->producer()->publish('order.created', '{"id":1001}');
$queue->producer()->publish('order.cancelled', '{"id":1002}');

$queue->worker()
    ->on('order.created', $createdHandler)
    ->on('order.cancelled', $cancelledHandler)
    ->on('*', $fallback)          // qualsiasi altro canale
    ->run();
```

Il **payload** (`text`) è arbitrario: JSON, dati strutturati o testo libero. La libreria non lo ricodifica mai e lo
decodifica in modo sicuro per l'handler: `DecodedText` è JSON (`kind: Json`, array invece di oggetti) quando il testo è JSON
valido, altrimenti la stringa grezza (`kind: Plain`). La decodifica non lancia mai eccezioni.

### 1.3 Punto-punto e consumer concorrenti

Esistono due famiglie di messaggistica:

- **Punto-punto**: ogni messaggio è elaborato da **un solo** consumer. Più consumer sulla stessa coda si *contendono* i
  messaggi, e questo distribuisce il carico.
- **Publish/subscribe (fan-out)**: ogni messaggio è copiato a ogni sottoscrittore.

queue-bridge implementa il **punto-punto con consumer concorrenti**. Per scalare, avvia più processi worker sulla stessa
coda: ogni messaggio va a uno solo di essi. Non c'è fan-out: per consegnare lo stesso evento a due servizi, pubblicalo su due
code.

```php
// Due processi con lo stesso codice si dividono il lavoro della coda "orders".
// php worker.php   (processo 1)
// php worker.php   (processo 2)
$queue->worker(new WorkerConfig(batchSize: 20))->on('order.created', $handler)->run();
```

### 1.4 Conferme e garanzie di consegna

Un consumer che ha finito un messaggio lo **conferma** (ack), e solo allora il broker lo cancella. Cosa succede se il
consumer muore prima dell'ack definisce la garanzia:

| Garanzia | Comportamento | Costo |
|---|---|---|
| at-most-once | ack *prima* dell'elaborazione: un crash perde il messaggio | mai duplicati |
| **at-least-once** | ack *dopo* l'elaborazione: con un crash il messaggio torna | possibili duplicati |
| exactly-once | richiede una transazione che comprenda broker ed effetti collaterali | in generale non ottenibile |

queue-bridge è **at-least-once**: il worker conferma solo dopo che l'handler è terminato, quindi nessun messaggio va perso,
nemmeno con un segfault, un out-of-memory o `kill -9`. Il prezzo è che un messaggio può essere elaborato due volte (l'handler
finisce, il processo muore prima dell'ack). Gli handler devono quindi essere **idempotenti**, oppure protetti da
`IdempotencyMiddleware`, che registra l'ID del messaggio in uno store e salta i messaggi già completati (un duplicato
ancora in elaborazione presso un altro worker viene ritentato allo scadere del lock di quel worker, mai confermato):

```php
$store = RedisIdempotencyStore::create($redisClient, $config);   // oppure InMemoryIdempotencyStore, o uno tuo

$worker = $queue->worker(new WorkerConfig(middlewares: [
    new IdempotencyMiddleware($store, lockTtlMs: 300_000, doneTtlMs: 86_400_000),
]));
```

### 1.5 Messaggi in elaborazione, orfani e riconsegna

Tra la consegna e l'ack un messaggio è **in elaborazione** (in flight): assegnato a un consumer e invisibile agli altri. Se
quel consumer muore, il messaggio diventa **orfano** e deve tornare in coda:

- **RabbitMQ** lo fa nativamente: quando la connessione (o il canale) si chiude, i messaggi non confermati tornano subito in
  coda.
- **Redis** tiene le entry in elaborazione nella pending list (PEL) del consumer group. Un'entry pending inattiva da più di
  `QueueConfig::$claimIdleMs` (default 5 minuti) viene ricopiata nello stream dalla manutenzione periodica di un qualsiasi
  worker (su Redis >= 8.4 la recupera direttamente `XREADGROUP ... CLAIM` durante la lettura).
- **InMemory** si comporta come Redis, con un orologio PSR-20.

`claimIdleMs` deve superare la durata dell'handler più lento, altrimenti un messaggio lento ma vivo viene scambiato per
orfano ed elaborato due volte in parallelo. Il worker lo verifica rispetto a `TimeoutMiddleware`.

Un intero batch è in elaborazione appena ricevuto, compresi i messaggi che aspettano il proprio turno. Su Redis e InMemory il
worker inizia quindi una consegna solo se è stata ricevuta da meno di `claimIdleMs - timeout dell'handler`
(`claimIdleMs / 2` senza `TimeoutMiddleware`); il resto del batch viene restituito con `release()`, dopo aver confermato i
messaggi già elaborati, e segnalato con un warning nel log. Se il warning compare, riduci `batchSize` o il timeout
dell'handler.

```php
new QueueConfig(name: 'reports', claimIdleMs: 900_000);   // gli handler possono durare fino a ~10 minuti
```

### 1.6 Retry e backoff

Un handler può fallire per un motivo transitorio (un timeout, un servizio che si riavvia). Ritentare subito di solito fallisce
di nuovo, quindi i tentativi vengono distanziati con un **backoff**: 1 s, 2 s, 4 s... più un **jitter** casuale, così molti
fallimenti non ritentano tutti nello stesso istante.

In queue-bridge un handler chiede un retry lanciando `RetryableException` (eventualmente con un proprio ritardo), oppure
qualsiasi `Throwable` se è attivo `retryAnyThrowable`. Il worker pubblica una nuova copia con `attempts + 1` dopo il ritardo
scelto dalla `RetryStrategyInterface` (`ExponentialBackoff` di default, `FixedDelay`, `NoRetry`), poi conferma l'originale: il
retry non va mai perso (script Lua atomico su Redis, publish-confirm-poi-ack su RabbitMQ).

```php
$worker = $queue->worker(new WorkerConfig(
    retryStrategy: new ExponentialBackoff(initialMs: 1000, multiplier: 2.0, maxMs: 300_000, jitter: 0.1),
))->on('invoice.send', static function (Message $message, DecodedText $text) use ($api): void {
    try {
        $api->send($text->value);
    } catch (ServiceUnavailable $e) {
        throw new RetryableException('API non disponibile', delayMs: 30_000, previous: $e);
    }
});
```

`QueueConfig::$maxAttempts` (default 3) è il numero di tentativi falliti dopo il quale il messaggio va nella dead letter
queue invece di essere ritentato.

### 1.7 Messaggi velenosi e dead letter queue

Un **messaggio velenoso** (poison message) non potrà mai essere elaborato: dati non validi, un bug per quel particolare input,
o un payload che fa crashare il processo ogni volta. Senza protezione verrebbe ritentato all'infinito e bloccherebbe un worker.

La **dead letter queue** (DLQ) è una coda separata dove questi messaggi vengono parcheggiati con il motivo del fallimento,
perché una persona o uno strumento li ispezioni, li corregga e li rimetta in coda. Un messaggio va nella DLQ quando:

- l'handler lancia `UnrecoverableMessageException` (o il payload non è decodificabile: `SerializationException`);
- l'handler fallisce e `attempts` raggiunge `maxAttempts`;
- il messaggio è stato **consegnato** più di `maxDeliveries` volte (default 10) senza mai essere confermato: fa crashare il
  worker prima di arrivare al `catch` (segfault, OOM, `kill -9`); su RabbitMQ il broker applica lo stesso limite con
  `x-delivery-limit`;
- è destinato a un canale senza handler (`UnknownChannelPolicy::DeadLetter`, il default) o è scaduto
  (`ExpiredMessagePolicy::DeadLetter`).

```php
$dlq = $queue->deadLetters();                  // DeadLetterAwareInterface, null se il backend non ha una DLQ
foreach ($dlq->deadLetters(20) as $deadLetter) {
    printf("%s %s %s\n", $deadLetter->message->id->value, $deadLetter->reason->kind->value, $deadLetter->reason->errorMessage);
}
$dlq->requeueDeadLetter($id);                  // torna in coda con attempts = 0, stesso ID
$dlq->requeueDeadLetters(100);
$dlq->purgeDeadLetters();
```

### 1.8 Messaggi differiti e scadenza

Un messaggio **differito** diventa visibile solo dopo un certo tempo (promemoria, retry programmati). Un messaggio con
**scadenza** (TTL) non vale più nulla dopo un termine (un codice monouso, un preventivo).

```php
$producer = $queue->producer();
$producer->publish('reminder.send', '{"user":42}', delayMs: 3_600_000);   // tra un'ora
$producer->publish('otp.send', '{"phone":"+39..."}', ttlMs: 60_000);      // inutile dopo un minuto
```

Redis tiene i differiti in un sorted set (ritardi esatti). RabbitMQ usa **code di attesa** a scaglioni fissi di TTL (1 s, 5 s,
30 s, 2 min, 10 min, 1 h di default): il ritardo è approssimato dagli scaglioni, e un messaggio tornato in anticipo viene
rimandato in attesa. Il worker controlla la scadenza prima di chiamare l'handler e conferma (`Discard`, default) o manda in
DLQ (`DeadLetter`) i messaggi scaduti.

### 1.9 Ordinamento

Le code vengono spesso pensate come FIFO, ma ordine stretto e consumer paralleli sono incompatibili: con due worker, il
messaggio 2 può finire prima del messaggio 1. Anche retry, differiti e recupero degli orfani riordinano i messaggi.
queue-bridge **non garantisce l'ordinamento**: `receive()` restituisce i messaggi in ordine di arrivo, ma l'ordine di
elaborazione non è garantito. Quando l'ordine conta, rendi gli handler commutativi (per esempio confrontando un numero di
versione) oppure usa un solo worker per coda e nessun retry.

### 1.10 Throughput: batch e prefetch

Ogni round trip verso il broker costa latenza, quindi producer e consumer lavorano a blocchi:

- `publishBatch()` invia molti messaggi in una pipeline Redis o in una serie di publish RabbitMQ con una sola attesa dei
  publisher confirm;
- `WorkerConfig::$batchSize` è quanti messaggi un worker riceve per round trip; gli ack di un batch vengono inviati insieme
  (`ackBatch`);
- su RabbitMQ `prefetch` (opzione del DTO, default 10) è quanti messaggi non confermati il broker spinge in anticipo a un
  consumer.

```php
$queue->producer()->publishBatch([
    new OutgoingMessage('order.created', '{"id":1}'),
    new OutgoingMessage('order.created', '{"id":2}', delayMs: 5_000),
]);
```

### 1.11 Ispezionare e svuotare una coda

Chi gestisce il sistema ha bisogno di vedere cosa è in attesa e, a volte, di buttarlo via (un deploy sbagliato che ha
pubblicato dati spazzatura):

```php
foreach ($queue->browse(50) ?? [] as $queued) {          // elenco senza consumare
    echo $queued->state->value, ' ', $queued->message->channel, PHP_EOL;  // in_flight | ready | delayed
}

$queue->purge();                                          // rimuove pronti + differiti, lascia in elaborazione e DLQ
```

Lo stesso dalla shell: `queue-worker queue:list --dsn=... --json` e `queue-worker queue:purge --dsn=... --force`.

### 1.12 Scegliere il backend

Il codice applicativo è lo stesso su ogni backend: cambia solo il DTO di strategia passato al factory.

```php
$queue = $factory->create(StrategyDtoRedis::fromPhpRedis($redis), $config);          // Redis
$queue = $factory->create(StrategyDtoRabbitMq::fromConnection($connection), $config); // RabbitMQ
$queue = $factory->create(new StrategyDtoInMemory(), $config);                       // test
```

Redis offre il throughput più alto e ritardi esatti; RabbitMQ la durabilità più forte (quorum queue replicate, riconsegna
immediata degli orfani, limite di consegne applicato dal broker); InMemory non richiede infrastruttura e ha un orologio
controllabile. Il README ha un confronto dettagliato.

---

## 2. Architettura

queue-bridge segue l'architettura **a porte e adattatori** (esagonale). Il core (producer, worker, middleware, retry, modello
del messaggio, codec) dipende solo da interfacce; ogni backend è un adattatore nella propria cartella (`Transport/<Backend>`)
e non dipende mai da un altro adattatore. `QueueBridge`, `Dsn/` e `Cli/` sono la composition root, l'unico codice che conosce
i backend concreti. Queste regole sono verificate da phpat dentro PHPStan.

```mermaid
flowchart TB
    subgraph Core["Core (agnostico rispetto al backend)"]
        P["Producer + middleware del producer"]
        W["Worker / MultiQueueWorker + middleware"]
        M["Message, Delivery, Envelope, QueuedMessage"]
        C["EnvelopeCodec (wire format v1)"]
        R["RetryStrategy, idempotenza, porte di System"]
    end
    T{{"TransportInterface + interfacce di capability (porta)"}}
    subgraph Adattatori
        RT["RedisTransport"]
        AT["RabbitMqTransport"]
        IT["InMemoryTransport"]
    end
    RC{{"RedisClientInterface"}}
    AC{{"AmqpConnectorInterface"}}
    P --> T
    W --> T
    T -.implementata da.-> RT
    T -.implementata da.-> AT
    T -.implementata da.-> IT
    RT --> RC
    AT --> AC
    RC -.-> PR["PhpRedisClient / PredisClient"]
    AC -.-> PA["PhpAmqpLibConnector"]
    Root["Composition root: QueueBridge, Dsn, Cli"] --> Core
    Root --> Adattatori
```

Pattern: **Strategy** per le differenze tra versioni di Redis (`Transport/Redis/Feature`), **Chain of Responsibility** per
scegliere il builder del transport dal DTO, **Middleware/Decorator** per gli aspetti trasversali, **Template Method** per i
contract test, **named constructor** per adattare i client nativi.

---

## 3. Diagrammi delle classi

### 3.1 Facade della coda, factory e DTO di strategia

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
    QueueFactory ..> Queue : crea
    QueueBridge ..> QueueFactory : crea
    Queue --> TransportInterface
```

`stats()`, `deadLetters()`, `purge()` e `browse()` restituiscono `null` quando il transport non implementa l'interfaccia di
capability corrispondente.

### 3.2 Porta del transport e capability

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

RabbitMQ non implementa `MaintenanceAwareInterface`: gli orfani tornano in coda nativamente quando il canale si chiude.

### 3.3 Modello del messaggio

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

### 3.4 Producer e worker

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

### 3.5 Adattatore Redis

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

### 3.6 Adattatore RabbitMQ

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
    PhpAmqpLibConnector ..> PhpAmqpLibChannel : apre
```

### 3.7 Eccezioni

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

## 4. Diagrammi di sequenza

### 4.1 Creazione di una coda (Redis, rilevamento automatico della versione)

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
    alt override superiore alla versione del server
        V-->>B: VersionMismatchException
    else INFO vietato dall'ACL e nessun override
        V-->>B: GE60 + log di warning
    else
        V-->>B: GE84
    end
    B->>B: FeatureSelector.select(capabilities)
    B-->>F: RedisTransport
    F-->>App: Queue (facade)
```

### 4.2 Pubblicazione di un messaggio

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant P as Producer
    participant MW as Middleware del producer
    participant T as Transport
    participant BK as Broker
    App->>P: publish(channel, text, headers, delayMs, ttlMs)
    P->>P: controllo dimensione del testo, nuovo UUIDv7, createdAt, expiresAt
    P->>MW: process(message, next)
    MW->>MW: aggiunge correlation_id, causation_id, traceparent
    MW->>P: next(message)
    P->>P: controllo dimensione degli header
    P->>T: send(message, delayMs)
    alt Redis, senza ritardo
        T->>BK: XADD stream * campi
    else Redis, differito
        T->>BK: ZADD delayed dueAtMs membro
    else RabbitMQ, senza ritardo
        T->>BK: basic.publish exchange, routing key = canale
        BK-->>T: publisher confirm
    else RabbitMQ, differito
        T->>BK: basic.publish coda di attesa dello scaglione, x-qb-due-at
        BK-->>T: publisher confirm
    end
    P-->>App: MessageId
```

### 4.3 Consumo: successo

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant T as Transport
    participant MW as Pipeline di middleware
    participant H as Handler
    loop fino a una condizione di uscita
        W->>T: maintain() se dovuta (Redis, InMemory)
        W->>T: receive(batchSize, receiveTimeoutMs)
        T-->>W: lista di Delivery
        loop ogni consegna
            W->>W: deliveryCount oltre maxDeliveries? scaduto? handler per il canale?
            W->>MW: process(message, next)
            MW->>H: handle(message, DecodedText)
            H-->>MW: ritorna
            MW-->>W: ritorna
            W->>W: mette in coda l'ack
        end
        W->>T: ackBatch(deliveries)
    end
```

### 4.4 Consumo: fallimento, retry e dead letter

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant H as Handler
    participant S as RetryStrategy
    participant T as Transport
    W->>H: handle(message)
    H-->>W: lancia RetryableException
    W->>W: emette MessageFailed, attempt = attempts + 1
    alt attempt inferiore a maxAttempts
        W->>S: nextDelayMs(attempt)
        S-->>W: 2000
        W->>T: retry(delivery, 2000)
        Note over T: Redis: un solo script Lua scrive la copia (attempts + 1) e conferma l'originale<br/>RabbitMQ: publish della copia, attesa del confirm, poi ack
        W->>W: emette MessageRetried
    else tentativi esauriti, o UnrecoverableMessageException
        W->>T: reject(delivery, FailureReason)
        Note over T: copia nella DLQ con il motivo, poi ack (stessa garanzia del retry)
        W->>W: emette MessageDeadLettered
    end
```

### 4.5 Crash del worker e recupero degli orfani su Redis

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1
    participant R as Redis
    participant W2 as Worker 2
    W1->>R: XREADGROUP GROUP queue-bridge worker-1 ... STREAMS stream >
    R-->>W1: entry 1-0 (ora nella PEL di worker-1)
    Note over W1: crash (segfault, OOM, kill -9): nessun ack
    loop ogni maintenanceIntervalMs
        W2->>R: maintain(): promuove i differiti, cerca le entry pending inattive
    end
    Note over R: entry inattiva da più di claimIdleMs
    W2->>R: EVALSHA requeueOrphans (ricontrolla l'inattività)
    R->>R: XADD della copia con deliveries = totale, ack + delete di 1-0
    W2->>R: XREADGROUP ... >
    R-->>W2: la copia, deliveryCount = consegne precedenti + 1
    Note over W2,R: Redis 8.4: XREADGROUP ... CLAIM claimIdleMs restituisce direttamente l'entry inattiva
```

### 4.6 Crash del worker su RabbitMQ

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1
    participant Q as Quorum queue
    participant W2 as Worker 2
    W1->>Q: basic.consume (prefetch N)
    Q-->>W1: basic.deliver tag 7
    Note over W1: crash: connessione TCP chiusa
    Q->>Q: messaggi non confermati rimessi in coda, x-delivery-count + 1
    Q-->>W2: basic.deliver (x-delivery-count = 1)
    W2->>W2: deliveryCount = 2, confrontato con maxDeliveries
    Note over Q: oltre x-delivery-limit è il broker stesso a spostare il messaggio nella DLQ
```

### 4.7 Arresto controllato

```mermaid
sequenceDiagram
    autonumber
    participant OS as Supervisor (systemd, k8s)
    participant SH as SignalHandler
    participant W as Worker
    participant T as Transport
    OS->>SH: SIGTERM
    SH->>W: stop()
    Note over W: il messaggio corrente viene completato e confermato
    loop il resto del batch già ricevuto
        W->>T: release(delivery)
        Note over T: di nuovo in coda, attempts invariato
    end
    W->>SH: reset()
    W-->>OS: run() restituisce StopRequested, exit code 0
```

### 4.8 Middleware di idempotenza

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant I as IdempotencyMiddleware
    participant S as IdempotencyStore
    participant H as Handler
    W->>I: process(message, next)
    I->>S: acquire(id, lockTtlMs)
    alt già completato
        S-->>I: Completed
        I-->>W: ritorna (il worker conferma il duplicato)
    else in elaborazione presso un altro worker
        S-->>I: InProgress(lockRemainingMs)
        I-->>W: RetryableException(delayMs = lockRemainingMs)
    else lock acquisito
        S-->>I: Acquired(LockToken)
        I->>H: next(message)
        alt l'handler riesce
            I->>S: complete(token, doneTtlMs)
        else l'handler lancia
            I->>S: release(token) (solo se il token corrisponde)
            I-->>W: rilancia
        end
    end
```

### 4.9 Svuotamento di una coda su Redis

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant T as RedisTransport
    participant R as Redis
    App->>T: purge()
    T->>R: EVALSHA purgeDelayed (ZCARD + DEL delayed)
    T->>R: XREVRANGE stream + - COUNT 1
    R-->>T: id più recente N
    loop pagine da 500 fino a N
        T->>R: EVALSHA purgeReady(group, start, N, 500)
        R->>R: XRANGE, XPENDING sull'intervallo, XDEL delle entry non pending
        R-->>T: eliminate, esaminate, ultimo id
    end
    T-->>App: differiti + eliminate
```

### 4.10 Elenco di una coda su RabbitMQ

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant T as RabbitMqTransport
    participant Q as Coda principale
    App->>T: browse(limit)
    T->>T: messaggi già prefetchati da questo consumer (pronti)
    loop fino al limite o alla coda vuota
        T->>Q: basic.get
        Q-->>T: messaggio (non confermato)
    end
    T->>Q: basic.nack requeue per ogni messaggio prelevato
    T-->>App: lista di QueuedMessage (Ready, o Delayed se x-qb-due-at è nel futuro)
```

### 4.11 Più code in un processo

```mermaid
sequenceDiagram
    autonumber
    participant M as MultiQueueWorker
    participant A as Worker orders
    participant B as Worker emails
    loop fino a una condizione di uscita dell'insieme
        M->>A: runOnce()
        A-->>M: elaborati
        M->>B: runOnce()
        B-->>M: elaborati
    end
    Note over M,B: SIGTERM: M.stop() chiama stop() su ogni worker interno, che restituisce i messaggi già ricevuti
```

---

## 5. Diagrammi di attività

Mermaid non ha un diagramma di attività UML dedicato, quindi le attività sono disegnate come flowchart con le convenzioni
UML: nodo iniziale/finale arrotondato, azioni come rettangoli, decisioni come rombi.

### 5.1 Ciclo principale del worker (`Worker::run()`)

```mermaid
flowchart TD
    S(["run()"]) --> R["registra l'handler di SIGTERM/SIGINT"]
    R --> C{"arresto richiesto?"}
    C -- sì --> X1["motivo = StopRequested"]
    C -- no --> L{"raggiunto maxMessages, maxRuntimeSeconds o memoryLimitMb?"}
    L -- sì --> X2["motivo = limite raggiunto"]
    L -- no --> M{"manutenzione dovuta? (maintenanceIntervalMs)"}
    M -- sì --> MT["transport.maintain()"]
    M -- no --> RC
    MT --> RC["receive(batch, receiveTimeoutMs)"]
    RC --> E{"nessun messaggio e stopWhenEmpty?"}
    E -- sì --> X3["motivo = QueueEmpty"]
    E -- no --> FE["per ogni consegna"]
    FE --> SR{"arresto richiesto nel frattempo?"}
    SR -- sì --> RL["release(delivery)"]
    SR -- no --> AG{"ricevuta troppo tempo fa per finire prima di claimIdleMs? (Redis, InMemory)"}
    AG -- sì --> RL
    AG -- no --> PR["process(delivery): vedi 5.2"]
    RL --> NX{"altre consegne?"}
    PR --> NX
    NX -- sì --> FE
    NX -- no --> FA["invia gli ack (ackBatch), gc_collect_cycles periodico"]
    FA --> C
    X1 --> END
    X2 --> END
    X3 --> END
    END["reset del signal handler, emette WorkerStopped"] --> F(["restituisce WorkerExitReason"])
```

### 5.2 Elaborazione di una consegna

```mermaid
flowchart TD
    S(["consegna"]) --> EV["emette MessageReceived"]
    EV --> D{"deliveryCount oltre maxDeliveries?"}
    D -- sì --> DLQ1["reject: MaxDeliveriesExceeded"]
    D -- no --> EXP{"scaduto?"}
    EXP -- sì --> EP{"expiredMessagePolicy"}
    EP -- Discard --> ACK1["ack + MessageDiscarded"]
    EP -- DeadLetter --> DLQ2["reject: Expired"]
    EXP -- no --> HC{"handler per il canale, o '*'?"}
    HC -- no --> UP{"unknownChannelPolicy"}
    UP -- Discard --> ACK2["ack + MessageDiscarded"]
    UP -- DeadLetter --> DLQ3["reject: UnknownChannel"]
    HC -- sì --> RUN["pipeline di middleware, poi handler(message, DecodedText)"]
    RUN --> OK{"eccezione?"}
    OK -- no --> ACK3["ack + MessageHandled"]
    OK -- sì --> K{"tipo di eccezione"}
    K -- "UnrecoverableMessageException / SerializationException" --> DLQ4["reject: Unrecoverable / Serialization"]
    K -- "RetryableException, o qualsiasi Throwable con retryAnyThrowable" --> A{"attempts + 1 inferiore a maxAttempts e ritardo disponibile?"}
    K -- "altro Throwable" --> DLQ5["reject: HandlerFailed"]
    A -- sì --> RT["retry(delivery, ritardo) + MessageRetried"]
    A -- no --> DLQ5
    DLQ1 --> F(["fatto"])
    DLQ2 --> F
    DLQ3 --> F
    DLQ4 --> F
    DLQ5 --> F
    ACK1 --> F
    ACK2 --> F
    ACK3 --> F
    RT --> F
```

### 5.3 Consegna differita su RabbitMQ (scaglioni di retry)

```mermaid
flowchart TD
    S(["send o retry con delayMs > 0"]) --> T["dueAt = adesso + delayMs"]
    T --> P["scaglione = il più grande non superiore al ritardo residuo (almeno il più piccolo)"]
    P --> W["publish nella coda di attesa rq.queue.retry.scaglione con x-qb-due-at"]
    W --> TTL["il messaggio attende il TTL dello scaglione"]
    TTL --> DL["dead-letter (at-least-once) verso la coda principale"]
    DL --> RCV["ricevuto da un consumer"]
    RCV --> Q{"x-qb-due-at nel futuro?"}
    Q -- sì --> P
    Q -- no --> H(["consegnato al worker"])
```

### 5.4 Scelta del livello Redis

```mermaid
flowchart TD
    S(["RedisTransportBuilder.build()"]) --> O{"override della versione nel DTO?"}
    O -- no --> I{"INFO server consentito?"}
    I -- no --> B["baseline GE60 + warning"]
    I -- sì --> D["livello = il più alto non superiore a redis_version"]
    O -- sì --> VV{"verifyVersion?"}
    VV -- no --> U["usa l'override"]
    VV -- sì --> I2{"INFO consentito?"}
    I2 -- no --> U
    I2 -- sì --> CMP{"override superiore al server?"}
    CMP -- sì --> E(["VersionMismatchException"])
    CMP -- no --> U
    B --> SEL
    D --> SEL
    U --> SEL["FeatureSelector: una strategia per capability"]
    SEL --> F(["RedisTransport"])
```

### 5.5 Svuotamento di una coda

```mermaid
flowchart TD
    S(["purge()"]) --> BK{"backend"}
    BK -- Redis --> R1["cancella il sorted set dei differiti"]
    R1 --> R2["legge l'id più recente dello stream N"]
    R2 --> R3{"stream vuoto?"}
    R3 -- sì --> F
    R3 -- no --> R4["pagina di 500 entry dal cursore fino a N"]
    R4 --> R5["cancella le entry non presenti nella PEL (quelle in elaborazione restano)"]
    R5 --> R6{"pagina piena?"}
    R6 -- sì --> R4
    R6 -- no --> F
    BK -- RabbitMQ --> Q1["queue.purge della coda principale e di ogni coda di attesa"]
    Q1 --> Q2{"questo transport sta consumando?"}
    Q2 -- sì --> Q3["legge i messaggi prefetchati e li conferma"]
    Q2 -- no --> F
    Q3 --> F
    BK -- InMemory --> M1["svuota le liste dei pronti e dei differiti"]
    M1 --> F(["restituisce il numero di messaggi rimossi"])
```

---

## 6. Specifiche

### 6.1 Messaggio

| Campo | Tipo | Regole |
|---|---|---|
| `id` | `MessageId` | UUIDv7 generato dal producer (monotono nello stesso millisecondo), mantenuto tra retry e riaccodamenti dalla DLQ |
| `channel` | `string` | 1–255 caratteri, `^[A-Za-z0-9._:-]+$` |
| `text` | `string` | UTF-8 valido, al più `QueueConfig::$maxTextBytes` byte (default 1 MiB, misurato con `strlen`) |
| `attempts` | `int` | esecuzioni fallite dell'handler finora, parte da 0, `+1` a ogni retry, riportato a 0 dal riaccodamento dalla DLQ |
| `createdAt` | `DateTimeImmutable` | UTC, dall'orologio PSR-20 |
| `expiresAt` | `?DateTimeImmutable` | `createdAt + ttlMs`; null = non scade mai |
| `headers` | `array<string,string>` | metadati (`correlation_id`, `causation_id`, `traceparent`...), dimensione totale al più `maxHeadersBytes` (default 64 KiB) |

`Delivery` aggiunge la `receipt` del backend (opaca per il core), `receivedAt` e `deliveryCount` (consegne della copia
corrente, crash inclusi; un retry crea una nuova copia e riparte da 1).

### 6.2 Decodifica del testo

1. `json_validate()` vero → `json_decode(..., associative: true, JSON_BIGINT_AS_STRING)` → `DecodedText(kind: Json)`; anche
   gli scalari (`123`, `true`, `"x"`, `null`) sono JSON.
2. Altrimenti, compresa la stringa vuota e il JSON più profondo della profondità massima (default 512) →
   `DecodedText(kind: Plain)` con il testo grezzo.
3. Non lancia mai eccezioni.

### 6.3 Wire format (versione 1)

`EnvelopeCodec` trasforma un `Message` in una mappa piatta `array<string,string>`: `v` (`"1"`), `id`, `channel`, `text`,
`attempts`, `created_at`, `expires_at`, `headers` (JSON). Legge tutte le versioni supportate e scrive sempre l'ultima; una
versione sconosciuta o campi non validi sollevano `SerializationException` e il messaggio va in DLQ senza raggiungere un
handler. Un nuovo formato sarà una nuova versione, con il decoder precedente mantenuto per almeno una major, così worker di
versioni diverse possono convivere durante un rolling deploy.

- **Redis**: la mappa è l'entry dello stream. Campi aggiuntivi: `deliveries` (consegne delle copie precedenti) e, nella DLQ,
  `failure_kind`, `failure_error_class`, `failure_message`, `failure_at`, `failure_original_id`.
- **RabbitMQ**: `text` è il body, gli altri campi sono header `x-qb-<campo>`, `message_id` è l'ID, la routing key è il
  canale. Header aggiuntivi: `x-qb-due-at`, `x-qb-failure-*`.

### 6.4 Configurazione

`QueueConfig` (uguale per ogni backend):

| Opzione | Default | Regola | Significato |
|---|---|---|---|
| `name` | `default` | `^[A-Za-z0-9._:-]+$` | nome della coda |
| `prefix` | `rq` | stesso pattern | prefisso delle chiavi Redis e delle risorse RabbitMQ |
| `maxTextBytes` | 1.048.576 | >= 1 | dimensione massima di `text` in byte |
| `maxHeadersBytes` | 65.536 | >= 0 | dimensione totale massima degli header |
| `maxAttempts` | 3 | >= 1 | tentativi falliti prima della DLQ |
| `maxDeliveries` | 10 | >= `maxAttempts + 1` | consegne senza ack prima della DLQ (protezione dai messaggi velenosi) |
| `claimIdleMs` | 300.000 | >= 1000 | inattività dopo la quale un messaggio in elaborazione è orfano (Redis, InMemory) |
| `dlqMaxLength` | 100.000 | >= 1 | retention della DLQ (Redis `MAXLEN ~`, InMemory) |

`WorkerConfig`:

| Opzione | Default | Significato |
|---|---|---|
| `batchSize` | 10 | messaggi per `receive()` |
| `receiveTimeoutMs` | 1000 | attesa bloccante quando la coda è vuota |
| `maxMessages` | null | esce dopo questo numero di messaggi (`MessageLimitReached`) |
| `maxRuntimeSeconds` | null | esce dopo questo tempo (`TimeLimitReached`) |
| `memoryLimitMb` | null | esce oltre questo uso di memoria (`MemoryLimitReached`) |
| `stopWhenEmpty` | false | esce alla prima ricezione vuota (`QueueEmpty`) |
| `maintenanceIntervalMs` | 5000 | ogni quanto gira `maintain()` (Redis, InMemory) |
| `gcEveryMessages` | 1000 | `gc_collect_cycles()` ogni N messaggi (0 = mai) |
| `retryStrategy` | `ExponentialBackoff` (1 s, ×2, max 300 s, jitter 10%) | ritardo del prossimo tentativo |
| `retryAnyThrowable` | false | ritenta qualsiasi `Throwable`, non solo `RetryableException` |
| `unknownChannelPolicy` | `DeadLetter` | `DeadLetter` o `Discard` |
| `expiredMessagePolicy` | `Discard` | `Discard` o `DeadLetter` |
| `middlewares` | `[]` | middleware del worker, il più esterno per primo |
| `listeners` | `[]` | `WorkerEventListenerInterface` (un listener che fallisce viene loggato, non ferma mai il worker) |

Verificato alla creazione del worker: `claimIdleMs` maggiore del timeout di `TimeoutMiddleware`.

### 6.5 Eventi del worker e motivi di uscita

Eventi: `MessageReceived`, `MessageHandled` (con la durata), `MessageFailed`, `MessageRetried`, `MessageDeadLettered`,
`MessageDiscarded`, `MessageReleased`, `WorkerStopped`.

`WorkerExitReason`: `StopRequested`, `MessageLimitReached`, `TimeLimitReached`, `MemoryLimitReached`, `QueueEmpty`. Le
eccezioni del backend non vengono catturate: escono da `run()` e il processo termina con un codice diverso da zero, così il
supervisor lo riavvia. Le eccezioni degli handler non fermano mai il worker.

### 6.6 Contratto del transport (tutti i backend)

- `send`/`sendBatch`: un batch fallito in parte lancia `BatchSendException` con gli ID inviati e quelli non inviati.
- `receive($max, $timeoutMs)`: al più `$max` messaggi in ordine di arrivo, lista vuota dopo il timeout.
- `ack`, `retry`, `reject`, `release` su una consegna non più posseduta (già confermata, o recuperata da un altro consumer)
  non hanno effetto.
- `retry` e `reject`: la nuova copia viene scritta prima di confermare l'originale: mai persa, al più duplicata.
- `release`: il messaggio è di nuovo disponibile subito, `attempts` invariato, `deliveryCount` non azzerato.
- Riaccodamento dalla DLQ: `attempts = 0`, stesso ID.
- `purge`: rimuove i messaggi pronti e differiti presenti alla chiamata; messaggi in elaborazione, DLQ e risorse del backend
  restano.
- `browse`: non consuma (ID e `attempts` invariati); in elaborazione, poi pronti in ordine di consegna, poi differiti per
  scadenza; le entry non decodificabili vengono saltate.

Queste regole sono verificate da `TransportContractTestCase`, eseguita su InMemory (suite unit) e su server reali Redis
6.0 → 8.4 e RabbitMQ 3.13 / 4.x (suite integration).

### 6.7 Matrice delle capability

| Capability | Redis | RabbitMQ | InMemory |
|---|---|---|---|
| Recupero degli orfani | esplicito (`maintain()`, o `XREADGROUP CLAIM` su 8.4) | nativo (chiusura del canale) | esplicito (`maintain()`) |
| Differiti | esatti (sorted set) | scaglioni di code di attesa | esatti |
| Conteggio delle consegne | sì (PEL + campo `deliveries`) | sì (`x-delivery-count`) | sì |
| Ack in batch | sì (un solo script Lua) | sì (un `basic.ack` per tag) | sì |
| Statistiche | pronti, in elaborazione, differiti, DLQ, lag (>= 7.0), consumer | pronti, in elaborazione di questo consumer, differiti, DLQ, consumer | pronti, in elaborazione, differiti, DLQ, lag |
| Svuotamento | differiti + entry dello stream non pending | `queue.purge` coda principale + code di attesa, prefetchati confermati | pronti + differiti |
| Elenco | in elaborazione, pronti, differiti (sola lettura) | solo pronti (`basic.get` + requeue) | in elaborazione, pronti, differiti |

### 6.8 Layout Redis e versioni

| Chiave (`prefix = rq`, coda `messages`) | Tipo | Scopo |
|---|---|---|
| `rq:{messages}:stream` | Stream | coda principale, consumer group `queue-bridge` |
| `rq:{messages}:delayed` | Sorted set | differiti, score = scadenza in ms, membro = JSON dei campi |
| `rq:{messages}:dlq` | Stream | dead letter queue (`MAXLEN ~ dlqMaxLength`) |
| `rq:{messages}:idem:{id}` | String + TTL | idempotenza (`RedisIdempotencyStore`) |

Gli hash tag tengono tutte le chiavi di una coda nello stesso slot di Redis Cluster (script Lua multi-key). Mai `MAXLEN` sullo
stream principale: il trim cancellerebbe messaggi non letti o pending; lo stream si svuota da solo con ack + delete.

| Livello | Comandi usati | Fallback sotto |
|---|---|---|
| GE60 | Streams, consumer group, `XPENDING` esteso, `EVALSHA` | — |
| GE62 | `XPENDING ... IDLE`, `XTRIM MINID` | filtro di inattività lato client, nessun trim di sicurezza |
| GE70 | lag in `XINFO GROUPS` | lag = null |
| GE80 | baseline (nessun comando stream nuovo) | come GE70 |
| GE82 | `XACKDEL`, `XTRIM ... ACKED` | `XACK` + `XDEL`, trim con `MINID` calcolato |
| GE84 | `XREADGROUP ... CLAIM`, `DELEX ... IFEQ` | script di manutenzione per rimettere in coda, rilascio del lock in Lua |

### 6.9 Topologia RabbitMQ

| Risorsa | Tipo | Argomenti |
|---|---|---|
| `rq.messages` (exchange) | topic, durevole | routing key = canale |
| `rq.messages` (coda) | quorum, durevole, binding `#` | `x-delivery-limit = maxDeliveries`, `x-dead-letter-strategy = at-least-once`, `x-overflow = reject-publish`, dead letter verso `rq.messages.dlq` |
| `rq.messages.retry.<ms>` | quorum, una per scaglione | `x-message-ttl = <ms>`, dead letter at-least-once verso la coda principale |
| `rq.messages.dlq` | classic, durevole | ispezione con `basic.get` + `nack` |

I messaggi sono persistenti e `mandatory`; i publisher confirm sono attivi di default. Le ricevute sono
`generazione:delivery-tag`, quindi dopo la riapertura del canale le ricevute vecchie non hanno effetto.

### 6.10 Eccezioni

| Eccezione | Quando | Reazione del worker |
|---|---|---|
| `ConnectionException` | backend non raggiungibile (dopo i tentativi di riconnessione del client) | esce da `run()` |
| `CommandFailedException` | un comando è stato rifiutato o la risposta non è valida | esce da `run()` |
| `VersionMismatchException` | override Redis superiore alla versione del server | in `create()` |
| `BatchSendException` | fallimento parziale di `sendBatch()` | — (lato producer) |
| `UnsupportedStrategyException` | nessun builder accetta il DTO | in `create()` |
| `SerializationException` | wire format non valido | DLQ (`Serialization`) |
| `UnrecoverableMessageException` | lanciata da un handler | DLQ (`Unrecoverable`), nessun retry |
| `RetryableException` | lanciata da un handler, `delayMs` opzionale | retry |
| `UnknownChannelException` | nessun handler per il canale | DLQ o scarto, secondo la politica |

### 6.11 CLI e DSN

Comandi di `bin/queue-worker`: `work --bootstrap=<file.php>`, `stats`, `ping`, `queue:list`, `queue:purge --force`,
`dlq:list`, `dlq:requeue [--id]`, `dlq:purge --force`. Exit code: `0` ok, `1` errore del backend, `2` errore d'uso.

DSN: `redis[s]://[user:pass@]host[:port][/db]?client=phpredis|predis&version=&verify_version=&consumer=&timeout=`,
`amqp[s]://user:pass@host[:port][/vhost]?prefetch=&confirms=&tiers=ms,ms&heartbeat=&timeout=`, `memory://`.

---

## 7. Concorrenza

### 7.1 Modello

Un worker è un **singolo processo PHP con un singolo thread**: riceve un batch ed elabora i suoi messaggi uno dopo l'altro.
Non fa mai fork e non usa thread. **Il parallelismo nasce dall'eseguire più processi** (su una o più macchine) sulla stessa
coda; il broker distribuisce i messaggi tra loro (consumer concorrenti). Così gli handler restano semplici (nessuna memoria
condivisa, nessun lock dentro il processo) e il sistema scala orizzontalmente.

### 7.2 Cosa garantisce che due worker non ricevano mai lo stesso messaggio

- **Redis**: `XREADGROUP ... >` è atomico sul server: ogni entry viene consegnata a un solo consumer del gruppo e registrata
  nella sua pending list. Ogni cambio di stato di una consegna (ack, retry, reject, release) è uno **script Lua** che prima
  verifica con `XPENDING` che l'entry appartenga ancora al consumer che chiama, poi scrive e conferma in un unico passo
  atomico. Un worker troppo lento che ha perso il messaggio per il recupero degli orfani non può confermare né ritentare la
  copia del nuovo proprietario.
- **RabbitMQ**: il broker consegna ogni messaggio a un solo consumer. I delivery tag valgono per canale; le ricevute portano
  la generazione del canale, quindi una ricevuta vecchia dopo una riconnessione viene ignorata invece di confermare il
  messaggio sbagliato.
- **InMemory**: un solo processo, stato condiviso in `InMemoryBroker`; `newConsumer()` crea un secondo consumer concorrente
  per i test.

Il contract test `testConcurrentConsumersNeverReceiveTheSameMessage` lo verifica su ogni backend.

### 7.3 Recupero degli orfani senza doppia elaborazione

- Più worker possono fare manutenzione nello stesso momento: lo script che rimette in coda **ricontrolla l'inattività dentro
  Lua**, quindi un orfano viene ricopiato una sola volta.
- `claimIdleMs` deve superare la durata massima di un handler (verificato rispetto a `TimeoutMiddleware`): altrimenti un
  messaggio ancora in elaborazione sarebbe considerato orfano ed elaborato di nuovo in parallelo.
- Su RabbitMQ non c'è una soglia di inattività: un messaggio torna solo quando il canale o la connessione del suo consumer si
  chiude. Tieni gli handler sotto il `consumer_timeout` del broker (default 30 minuti) e usa gli heartbeat
  (`PCNTLHeartbeatSender` con `ext-pcntl`), altrimenti il broker chiude il canale e il messaggio viene riconsegnato.

### 7.4 I duplicati sono possibili: idempotenza

At-least-once significa che un messaggio può essere elaborato due volte: crash dopo l'effetto collaterale e prima dell'ack,
un orfano recuperato mentre il worker originale era solo lento, un publish confermato dal broker dopo un timeout di rete.
`IdempotencyMiddleware` serializza i duplicati concorrenti con un **lock a due fasi** in uno store condiviso:

1. `acquire(id, lockTtlMs)`: `SET NX PX` su Redis, in uno script Lua che distingue atomicamente i tre esiti:
   - `Acquired`: si esegue l'handler;
   - `Completed`: il duplicato viene confermato senza eseguire l'handler;
   - `InProgress`: il lock è di un altro worker. Il messaggio **non** viene confermato (quel worker potrebbe ancora fallire
     o morire, e il messaggio andrebbe perso): `RetryableException` con ritardo pari al tempo residuo del lock. Il retry
     consuma un tentativo di `maxAttempts`;
2. handler;
3. `complete(token, doneTtlMs)` lo segna come fatto con un TTL lungo; in caso di errore `release(token)` lo libera solo se il
   token corrisponde ancora (compare-and-delete: `DELEX ... IFEQ` su Redis 8.4, uno script Lua prima).

Lo store è indipendente dal transport: con RabbitMQ passa un `RedisIdempotencyStore` o una tua implementazione.

### 7.5 Producer

I producer non hanno stato a parte il generatore di ID, e un numero qualsiasi di processi può pubblicare in parallelo. Gli
ID UUIDv7 sono univoci tra processi (parte casuale) e monotoni dentro un processo. Un batch non è una transazione: se una
parte fallisce, `BatchSendException` elenca cosa è stato inviato, così chi chiama può reinviare solo il resto.

### 7.6 Manutenzione, svuotamento ed elenco sotto carico

- La promozione dei differiti è un solo script Lua per chiamata (fino a 100 messaggi), quindi due worker che promuovono
  nello stesso momento non duplicano mai un differito.
- `purge()` su Redis lavora a pagine di 500 entry fino all'ID più recente letto all'inizio: non blocca Redis con arretrati
  grandi e non insegue i messaggi pubblicati mentre gira. Le entry nella pending list vengono saltate, quindi i messaggi in
  elaborazione non vengono mai cancellati sotto i piedi di un worker.
- `browse()` è un'istantanea: con worker attivi gli stati cambiano mentre leggi. Su RabbitMQ `browse()` preleva i messaggi
  con `basic.get`, quindi per un breve momento sono invisibili agli altri consumer; vengono restituiti con `nack` + requeue.

### 7.7 Cosa non viene gestito

- **Ordinamento**: non garantito (retry, differiti, consumer paralleli).
- **Serializzazione per chiave** (tutti i messaggi dello stesso cliente in sequenza): non fornita; usa una coda per
  partizione, o lock nell'handler.
- **Exactly-once**: non fornito; usa handler idempotenti o `IdempotencyMiddleware`.
- **Fan-out**: un consumer group / una coda per `QueueConfig`.

---

## 8. Eseguire il worker su un server

### 8.1 Principi

- Il worker è un **processo sempre attivo**, non un cron per messaggio: tiene aperta la connessione e attende sul broker
  (`receiveTimeoutMs`), quindi la latenza è di millisecondi e non c'è un costo di avvio per messaggio.
- Eseguilo sotto un **supervisore di processi** (systemd, Supervisor, Kubernetes, restart policy di Docker) che lo riavvia
  quando esce.
- Fallo **uscire di proposito a intervalli regolari** con `--max-runtime` e `--memory-limit`: i processi PHP accumulano
  memoria e stato vecchio; un'uscita pulita seguita da un riavvio è il rimedio più semplice. Le uscite sono controllate.
- Scala eseguendo **N processi identici**; ognuno è un consumer concorrente.
- Fermalo con **SIGTERM** e dagli più tempo dell'handler più lento prima del SIGKILL: il messaggio corrente viene
  completato, quelli già ricevuti vengono restituiti con `release()`.
- `ext-pcntl` serve per i segnali e per `TimeoutMiddleware`; `QueueBridge::factory()` attiva la gestione dei segnali quando
  l'estensione è caricata.

### 8.2 Il file di bootstrap

La CLI carica un file PHP che restituisce una closure che registra gli handler:

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

Lo stesso si può fare in PHP con `$queue->worker(...)->on(...)->run()` (vedi `examples/08-worker-in-code.php`), per esempio
dentro un comando console di un framework.

### 8.3 systemd

Una unit template esegue N istanze (`queue-worker@1`, `queue-worker@2`, ...). Il nome del consumer Redis dovrebbe essere
stabile e univoco per istanza, così la sua pending list sopravvive ai riavvii:

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

Una copia pronta è in [examples/deploy/queue-worker.service](examples/deploy/queue-worker.service).

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

Vedi [examples/deploy/supervisord.conf](examples/deploy/supervisord.conf).

### 8.5 Kubernetes

Un container esegue un processo worker; le repliche danno il parallelismo. Usa il nome del pod come nome del consumer Redis
e un periodo di grazia alla terminazione più lungo dell'handler più lento.

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

Il container si riavvia dopo `--max-runtime` (restart policy `Always`). Tieni il limite di memoria del container sopra
`--memory-limit`, così il worker esce da solo prima che il kernel lo uccida.

### 8.6 Cron (solo per code a basso volume o batch)

Quando un processo permanente non è possibile (hosting condiviso), esegui il worker periodicamente e lascialo uscire quando
la coda è vuota, con un lock perché le esecuzioni non si sovrappongano:

```
* * * * * flock -n /tmp/queue-orders.lock php /var/www/app/vendor/bin/queue-worker work --queue=orders \
    --bootstrap=/var/www/app/config/queue-handlers.php --stop-when-empty --max-runtime=55
```

La latenza arriva a un minuto, e il recupero degli orfani e la promozione dei differiti avvengono solo mentre un worker gira.

### 8.7 Quanti worker, e come dimensionarli

- Parti con **un processo per core** per handler che usano la CPU, di più per handler di I/O (chiamate HTTP, e-mail), e
  misura: se `stats()->ready` cresce stabilmente, aggiungi processi; se i worker sono spesso inattivi, toglili.
- `batchSize` 10–50 è un buon default; su RabbitMQ tieni `prefetch` vicino a `batchSize`.
- Più code piccole con poco traffico possono condividere un processo con `MultiQueueWorker` (worker interni con un
  `receiveTimeoutMs` basso, es. 100 ms).
- Handler lenti: alza `claimIdleMs` sopra la durata massima e proteggili con `TimeoutMiddleware`.

### 8.8 Checklist operativa

| Voce | Raccomandazione |
|---|---|
| Redis | AOF attivo (`appendonly yes`, `appendfsync everysec`), `maxmemory-policy noeviction`, un nome di `consumer` stabile per processo |
| RabbitMQ | quorum queue (create da `setup()`), heartbeat, handler sotto il `consumer_timeout` |
| Setup | `$queue->setup()` (o il comando CLI `work`, che lo chiama) al deploy; è idempotente |
| Salute | readiness: `queue-worker ping`; allarmi su `stats` (`ready` in crescita, `deadLetters` > 0, `lag`) |
| Log | logger PSR-3; `-v`/`-vv` nella CLI; `LoggingMiddleware` per i log per messaggio |
| Metriche | un `WorkerEventListenerInterface` che conta messaggi elaborati/falliti/ritentati/in DLQ e durate |
| DLQ | controllala regolarmente: `queue-worker dlq:list`, correggi, poi `dlq:requeue` |
| Deploy | i riavvii progressivi sono sicuri: SIGTERM restituisce il lavoro in corso e il wire format è versionato |
| Emergenze | `queue-worker queue:purge --force` svuota pronti e differiti, `dlq:purge --force` la DLQ |
