# queue-bridge

[English](README.md) | **Italiano**

Coda di messaggi per PHP 8.3+ indipendente dal backend: un'unica API tipizzata per **Redis Streams** e **RabbitMQ**, con
retry, dead letter queue, messaggi differiti e rilevamento automatico della versione di Redis. Un backend **InMemory** con
la stessa semantica serve per i test e lo sviluppo locale.

```php
$queue = QueueBridge::factory()->create(StrategyDtoRedis::fromPhpRedis($redis), new QueueConfig(name: 'orders'));

$queue->producer()->publish('order.created', '{"id":1001,"total":"12.90"}');

$queue->worker()->on('order.created', static function (Message $message, DecodedText $text): void {
    // $text->value vale ['id' => 1001, 'total' => '12.90']
})->run();
```

Passare a RabbitMQ cambia una riga: `StrategyDtoRabbitMq::fromConnection($connection)` al posto di
`StrategyDtoRedis::fromPhpRedis($redis)`.

- **Consegna at-least-once**: nessun messaggio va perso se un worker si blocca (eccezione, segfault, OOM, `kill -9`); al
  peggio viene consegnato due volte.
- **Payload arbitrari**: il testo può essere JSON, dati strutturati o testo libero; viene decodificato in modo sicuro (JSON
  se valido, altrimenti la stringa grezza) e non lancia mai eccezioni.
- **Redis 6.0 → 8.4**: la versione del server viene rilevata e vengono usati i comandi migliori (`XACKDEL`,
  `XTRIM ... ACKED`, `XREADGROUP ... CLAIM`, `DELEX`) senza cambiare l'API pubblica.
- **RabbitMQ 3.13 / 4.x**: quorum queue, publisher confirms, `x-delivery-limit` con dead lettering at-least-once.
- **Worker**: handler per canale, middleware, strategie di retry, protezione dai messaggi velenosi (`maxDeliveries`),
  scadenza dei messaggi, arresto controllato, eventi per le metriche, più code nello stesso processo e una CLI.

## Indice

- [Installazione](#installazione)
- [Redis, RabbitMQ o InMemory?](#redis-rabbitmq-o-inmemory)
- [Integrazione](#integrazione): [Redis](#redis), [RabbitMQ](#rabbitmq), [InMemory](#inmemory), [DSN](#da-un-dsn)
- [Pubblicare messaggi](#pubblicare-messaggi)
- [Consumare con il Worker](#consumare-con-il-worker)
- [Consumare senza il Worker](#consumare-senza-il-worker)
- [CLI](#cli)
- [Esempi](#esempi)
- [Configurazione](#configurazione)
- [Configurazione consigliata dei server](#configurazione-consigliata-dei-server)
- [Garanzie di consegna e limiti](#garanzie-di-consegna-e-limiti)
- [Supporto delle versioni di Redis](#supporto-delle-versioni-di-redis)
- [Benchmark](#benchmark)
- [Estendere](#estendere)
- [Sviluppo](#sviluppo)

La documentazione di progetto completa (concetti delle code, diagrammi UML, concorrenza, schedulazione del worker) è in
[DOC_IT.md](DOC_IT.md).

## Installazione

```bash
composer require snipershady/queue-bridge
```

Poi installa il client del backend che usi:

| Backend | Client |
|---|---|
| Redis | `ext-redis` (consigliata) oppure `composer require predis/predis` |
| RabbitMQ | `composer require php-amqplib/php-amqplib` |
| InMemory | niente |
| Arresto controllato su SIGTERM/SIGINT, timeout degli handler | `ext-pcntl` |

Requisiti: PHP 8.3, 8.4 o 8.5; Redis >= 6.0 (Valkey e i servizi gestiti che dichiarano `redis_version` 7.2 funzionano come
Redis 7.0); RabbitMQ 3.13 o 4.x.

## Redis, RabbitMQ o InMemory?

I tre backend implementano lo stesso contratto e superano gli stessi contract test: il codice applicativo non cambia.
Differiscono nel modo in cui ottengono le garanzie e in quanto costa farli funzionare.

| | Redis (Streams) | RabbitMQ (quorum queue) | InMemory |
|---|---|---|---|
| Persistenza | AOF (`appendfsync everysec`: fino a ~1 s di scritture perse se il server si blocca) | su disco, fsync prima del publisher confirm | nessuna: persa all'uscita del processo |
| Alta affidabilità | replica + Sentinel, oppure Redis Cluster (le chiavi usano hash tag) | replica Raft su 3+ nodi | — |
| Dimensione del backlog | deve stare in RAM | su disco, oltre la RAM | memoria del processo |
| Recupero dopo il crash di un worker | dopo `claimIdleMs` (default 5 min) | immediato, alla caduta della connessione | dopo `claimIdleMs` |
| Messaggi differiti | esatti (sorted set) | code di attesa a scaglioni fissi, precisione ≈ uno scaglione | esatti, guidati da un orologio PSR-20 |
| Protezione dai messaggi velenosi | `maxDeliveries` | `maxDeliveries` + `x-delivery-limit` del broker | `maxDeliveries` |
| Throughput (un processo, publish in batch / consume) | ~115.000 / ~30.000 msg/s | ~25.000 / ~6.000 msg/s | ~330.000 / ~68.000 msg/s |
| Statistiche | pronti, in elaborazione, differiti, DLQ, lag (>= 7.0), consumer | pronti, in elaborazione (di questo consumer), differiti, DLQ, consumer | pronti, in elaborazione, differiti, DLQ, lag |
| Store di idempotenza | `RedisIdempotencyStore` sullo stesso server | serve Redis o uno store proprio | `InMemoryIdempotencyStore` |
| Elenco della coda (`browse`) | in elaborazione, pronti e differiti, in sola lettura | solo i pronti, letti con `basic.get` + requeue | in elaborazione, pronti e differiti |
| Uso tipico | throughput alto, latenza bassa, Redis già presente | durabilità e replica prima di tutto, RabbitMQ già presente | test unitari, sviluppo locale |

| Backend | Vantaggi | Svantaggi |
|---|---|---|
| **Redis** | Throughput più alto e latenza più bassa; differiti esatti; un solo componente spesso già presente (cache, sessioni); store di idempotenza e coda sullo stesso server; lag dei consumer disponibile dalla 7.0; testato dalla 6.0 alla 8.4 con ottimizzazioni specifiche per versione | La durabilità dipende dalla configurazione AOF; il backlog deve stare in memoria e `maxmemory-policy` deve essere `noeviction`; i messaggi di un worker bloccato attendono `claimIdleMs` prima di passare a un altro worker; l'alta affidabilità richiede Sentinel o Cluster |
| **RabbitMQ** | Durabilità più forte: quorum queue replicate e publisher confirms; i messaggi non confermati tornano subito quando un worker muore; il broker applica un limite di consegne anche se il worker non ci riesce; backlog più grande della RAM; interfaccia di gestione e strumenti operativi maturi | Throughput più basso per processo; differiti arrotondati agli scaglioni di retry; un'infrastruttura in più (tre nodi per l'alta affidabilità); heartbeat e `consumer_timeout` limitano la durata degli handler; nessuna statistica sul lag; l'idempotenza richiede uno store esterno |
| **InMemory** | Nessun servizio; deterministico con un orologio mosso a mano (differiti, retry e scadenze senza attese); semantica completa, è l'implementazione di riferimento del contratto; il più veloce | Non persistente e non condiviso tra processi: solo per test e sviluppo |

## Integrazione

`QueueBridge::factory()` restituisce un factory che riceve **un DTO tipizzato** per backend e una `QueueConfig`. Tutto il
resto (producer, worker, handler, middleware) è identico su ogni backend.

### Redis

```php
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\Transport\Redis\RedisIdempotencyStore;
use snipershady\Queue\Transport\Redis\StrategyDtoRedis;

// ext-redis: una factory (al posto di un'istanza) permette al client di riconnettersi dopo un errore di rete.
$strategy = StrategyDtoRedis::fromPhpRedis(static function (): Redis {
    $redis = new Redis();
    $redis->connect('127.0.0.1', 6379, 2.0);
    $redis->auth(['app', 'secret']); // utente ACL, facoltativo

    return $redis;
});

// oppure Predis, che si riconnette da solo
$strategy = StrategyDtoRedis::fromPredis(new Predis\Client('tcp://127.0.0.1:6379'));

// In produzione dai a ogni processo un nome di consumer stabile (nome dell'host o del pod).
$strategy = new StrategyDtoRedis($strategy->client, consumer: gethostname());

$config = new QueueConfig(name: 'orders', claimIdleMs: 60_000);
$queue = QueueBridge::factory(logger: $logger)->create($strategy, $config);
$queue->setup(); // crea il consumer group; idempotente

// Store di idempotenza sullo stesso Redis, per IdempotencyMiddleware.
$store = RedisIdempotencyStore::create($strategy->client, $config);
```

La versione di Redis viene rilevata da `INFO server`. Se l'ACL vieta `INFO`, passala tu: `StrategyDtoRedis::fromPhpRedis($redis,
RedisVersionEnum::GE70)`; un override superiore al server lancia `VersionMismatchException` in `create()`.

### RabbitMQ

```php
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use snipershady\Queue\Transport\RabbitMq\RetryTiers;
use snipershady\Queue\Transport\RabbitMq\StrategyDtoRabbitMq;

// Factory della connessione: il connector riapre il canale dopo ogni errore AMQP e la connessione dopo una caduta.
$strategy = StrategyDtoRabbitMq::fromConnectionFactory(
    static fn (): AbstractConnection => new AMQPStreamConnection('rabbitmq', 5672, 'app', 'secret'),
);

// Scaglioni di retry e prefetch personalizzati.
$strategy = new StrategyDtoRabbitMq(
    new PhpAmqpLibConnector(static fn (): AbstractConnection => new AMQPStreamConnection('rabbitmq', 5672, 'app', 'secret')),
    retryTiers: new RetryTiers([1_000, 10_000, 60_000, 600_000]),
    prefetch: 50,
);

$queue = QueueBridge::factory()->create($strategy, new QueueConfig(name: 'orders'));
$queue->setup(); // exchange, quorum queue, code di attesa e DLQ; idempotente
```

`StrategyDtoRabbitMq::fromConnection($connection)` e `fromPhpAmqpLib($channel)` riusano una connessione esistente.

### InMemory

```php
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;

$clock = new MyTestClock(); // qualsiasi orologio PSR-20 che puoi far avanzare
$queue = QueueBridge::factory(clock: $clock)->create(new StrategyDtoInMemory($clock), new QueueConfig(name: 'orders'));

$queue->producer()->publish('mail', 'welcome', delayMs: 60_000);
$clock->advance(60_000); // il messaggio differito è disponibile, senza attese
```

### Da un DSN

Comodo per file di configurazione e variabili d'ambiente:

```php
use snipershady\Queue\Dsn\StrategyDtoFactory;

$queue = QueueBridge::factory()->create((new StrategyDtoFactory())->fromDsn(getenv('QUEUE_DSN')), new QueueConfig(name: 'orders'));
```

```
redis://[user:password@]host[:port][/db][?opzioni]      rediss:// per TLS
amqp://[user:password@]host[:port][/vhost][?opzioni]    amqps:// per TLS
memory://
```

| Backend | Opzione | Default | Significato |
|---|---|---|---|
| Redis | `client` | `phpredis` se `ext-redis` è caricata, altrimenti `predis` | `phpredis` o `predis` |
| Redis | `version` | rilevata | forza un livello: `6.0`, `6.2`, `7.0`, `8.0`, `8.2`, `8.4` |
| Redis | `verify_version` | `true` | rifiuta un override superiore al server |
| Redis | `consumer` | casuale | nome del consumer nel gruppo |
| Redis | `timeout` | `2.0` | timeout di connessione in secondi |
| RabbitMQ | `prefetch` | `10` | prefetch di `basic_qos` |
| RabbitMQ | `confirms` | `true` | publisher confirms |
| RabbitMQ | `tiers` | `1000,5000,30000,120000,600000,3600000` | scaglioni di retry/differimento in ms |
| RabbitMQ | `heartbeat` | `0` | heartbeat in secondi |
| RabbitMQ | `timeout` | `3.0` | timeout di connessione in secondi |

Le credenziali vanno codificate per URL. Le opzioni booleane accettano `1/0`, `true/false`, `yes/no`, `on/off`. Il vhost
AMQP di default è `/`. Esempio: `redis://app:secret@cache:6379/2?consumer=web-1&timeout=1.5`.

## Pubblicare messaggi

```php
$producer = $queue->producer();

$id = $producer->publish('orders', $json);                           // restituisce il MessageId (UUIDv7)
$producer->publish('emails', $text, headers: ['tenant' => 'acme']);
$producer->publish('reminders', $text, delayMs: 60_000);            // non consegnato prima di un minuto
$producer->publish('quotes', $text, ttlMs: 30_000);                 // non elaborato se più vecchio di 30 s

$ids = $producer->publishBatch([
    new OutgoingMessage('orders', $first),
    new OutgoingMessage('orders', $second, delayMs: 5_000),
]);
```

- `text` è una qualsiasi stringa UTF-8 valida fino a `maxTextBytes` (default 1 MiB): JSON, dati strutturati o testo libero.
- I nomi dei canali rispettano `^[A-Za-z0-9._:-]+$` (massimo 255 caratteri).
- `publishBatch()` usa una sola pipeline su Redis e confirm in batch su RabbitMQ. Un fallimento parziale lancia
  `BatchSendException` con gli ID inviati e quelli falliti.
- Middleware del producer: `CorrelationIdMiddleware` aggiunge `correlation_id`/`causation_id` dal messaggio in
  elaborazione, `TraceContextMiddleware` aggiunge un `traceparent` W3C. Passali a
  `QueueBridge::factory(producerMiddlewares: [...])`.

## Consumare con il Worker

```php
$worker = $queue->worker(new WorkerConfig(
    batchSize: 10,
    receiveTimeoutMs: 1000,
    maxMessages: 10_000,        // esce dopo N messaggi,
    maxRuntimeSeconds: 3600,    // o dopo un'ora,
    memoryLimitMb: 256,         // o oltre questa memoria: il supervisor riavvia il processo
    retryStrategy: new ExponentialBackoff(),
    middlewares: [new LoggingMiddleware($logger), new IdempotencyMiddleware($store)],
    listeners: [$metrics],      // WorkerEventListenerInterface
));

$worker
    ->on('orders', new OrderHandler())                       // HandlerInterface
    ->on('emails', static function (Message $message, DecodedText $text): void { /* ... */ })
    ->on('*', $fallback);                                    // canali senza un handler dedicato

$reason = $worker->run(); // WorkerExitReason
```

L'handler riceve il `Message` (id, canale, testo grezzo, tentativi, header, date) e un `DecodedText`: `$text->isJson()`,
`$text->value` (JSON decodificato con gli oggetti come array, oppure la stringa grezza) e `$text->raw`. Puoi anche
ignorarlo e decodificare `$message->text` da solo.

| Esito dell'handler | Risultato |
|---|---|
| termina | ack |
| lancia `RetryableException` (qualsiasi `Throwable` con `retryAnyThrowable: true`) | retry dopo il ritardo della strategia (o il `delayMs` dell'eccezione); DLQ dopo `maxAttempts` |
| lancia `UnrecoverableMessageException`; payload in un formato sconosciuto | subito in DLQ |
| il worker si blocca prima dell'ack | consegnato di nuovo; DLQ dopo `maxDeliveries` consegne |
| messaggio scaduto (`ttlMs`) | scartato, oppure DLQ con `expiredMessagePolicy: ExpiredMessagePolicy::DeadLetter` |
| nessun handler per il canale | DLQ, oppure scartato con `unknownChannelPolicy: UnknownChannelPolicy::Discard` |

- **Arresto controllato**: su SIGTERM/SIGINT (con `ext-pcntl`) o `stop()` (dichiarato in `WorkerInterface`) il worker termina il messaggio corrente,
  restituisce quelli già ricevuti e ritorna `WorkerExitReason::StopRequested`. `stopWhenEmpty: true` esce alla prima
  ricezione vuota (job batch, cron).
- **Middleware**: `LoggingMiddleware`, `TimeoutMiddleware` (richiede `ext-pcntl`; un timeout viene ritentato),
  `IdempotencyMiddleware` (lock a due fasi sull'ID del messaggio con `InMemoryIdempotencyStore`,
  `RedisIdempotencyStore::create()` o una tua `IdempotencyStoreInterface`, su qualsiasi backend),
  `CorrelationContextMiddleware`.
- **Eventi**: `MessageReceived`, `MessageHandled`, `MessageFailed`, `MessageRetried`, `MessageDeadLettered`,
  `MessageDiscarded`, `MessageReleased`, `WorkerStopped`.
- **Più code in un processo**: `MultiQueueWorker` esegue un giro di ogni worker a turno, così una coda lenta non blocca le
  altre; dai ai worker interni un `receiveTimeoutMs` breve (es. 100 ms).

```php
$multi = new MultiQueueWorker(
    [$ordersWorker, $emailsWorker],
    new WorkerConfig(maxRuntimeSeconds: 3600),
    signalHandler: new PcntlSignalHandler(),
);
$multi->run();
```

Dead letter, statistiche e health check:

```php
$queue->ping();                      // lancia ConnectionException: usalo come readiness probe
$stats = $queue->stats();            // ready, inFlight, delayed, deadLetters, lag, consumers
$dlq = $queue->deadLetters();        // DeadLetterAwareInterface
$dlq->deadLetters(20);               // list<DeadLetter>: messaggio + FailureReason (tipo, classe dell'errore, messaggio, data)
$dlq->requeueDeadLetter($id);        // torna in coda con attempts = 0
$dlq->requeueDeadLetters(100);
$dlq->purgeDeadLetters();
```

Elenco e svuotamento della coda:

```php
foreach ($queue->browse(50) ?? [] as $queued) {   // null se il backend non sa elencare i messaggi
    // QueuedMessage: message, state (InFlight | Ready | Delayed), availableAt (solo per i differiti)
    echo $queued->message->id->value, ' ', $queued->state->value, PHP_EOL;
}

$removed = $queue->purge();                      // pronti + differiti; null se il backend non sa svuotare la coda
```

- `browse()` non consuma mai: ID e `attempts` non cambiano. Ordine: in elaborazione, pronti, differiti per scadenza. È
  un'istantanea: con worker attivi i messaggi possono cambiare stato mentre li leggi.
- `purge()` rimuove i messaggi pronti e differiti presenti al momento della chiamata. I messaggi in elaborazione non
  vengono toccati (sono confermati o tornano in coda come sempre), la dead letter queue ha il suo `purgeDeadLetters()` e
  la coda resta utilizzabile.
- **RabbitMQ**: AMQP non permette di leggere una coda senza consumarla, quindi `browse()` preleva i messaggi pronti con
  `basic.get` e li restituisce con `nack` + requeue. I messaggi nelle code di attesa e quelli in mano ad altri consumer
  non sono visibili; su RabbitMQ 3.13 ogni browse incrementa `x-delivery-count` (un uso frequente avvicina i messaggi a
  `x-delivery-limit` e alla DLQ) e i messaggi letti da un `browse()` che non arriva alla fine della coda tornano in fondo.
  Usalo per ispezioni occasionali, non per il polling.

## Consumare senza il Worker

Per inserire la coda in un demone o in un event loop esistente, usa direttamente il transport e decidi tu cosa fare di
ogni messaggio:

```php
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Transport\Capability\MaintenanceAwareInterface;

$transport = $queue->transport();
$decoder = new TextDecoder();

while ($running) {
    if ($transport instanceof MaintenanceAwareInterface) {
        $transport->maintain(); // Redis: promuove i differiti, recupera gli orfani
    }

    foreach ($transport->receive(max: 10, timeoutMs: 1000) as $delivery) {
        $decoded = $decoder->decode($delivery->message->text); // non lancia mai eccezioni

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
            $transport->retry($delivery, 5_000); // nuova copia con attempts + 1, consegnata tra 5 s
        }
    }
}
```

Senza il Worker ti fai carico di quello che fa lui: `maxAttempts` (`$delivery->message->attempts`), `maxDeliveries`
(`$delivery->deliveryCount`), scadenza (`$delivery->message->expiresAt`), segnali e limiti.
`$transport->release($delivery)` restituisce un messaggio senza contare un fallimento.

## CLI

```
vendor/bin/queue-worker <comando> --dsn=<dsn> [--queue=<nome>] [--prefix=<prefisso>] [opzioni]
```

| Comando | Cosa fa |
|---|---|
| `work --bootstrap=<file.php>` | avvia un worker; il file restituisce una `Closure(WorkerInterface): void` che registra gli handler ([esempio](examples/03-worker-bootstrap.php)). Opzioni: `--batch-size`, `--timeout` (ms), `--max-messages`, `--max-runtime` (s), `--memory-limit` (MB), `--stop-when-empty` |
| `stats [--json]` | statistiche della coda |
| `ping` | health check del backend |
| `dlq:list [--limit=20] [--json]` | dead letter con il motivo del fallimento |
| `dlq:requeue [--limit=100 \| --id=<uuid>]` | rimette in coda le dead letter (fino al limite) o una sola |
| `dlq:purge --force` | cancella tutte le dead letter |
| `queue:list [--limit=20] [--json]` | messaggi in coda (in elaborazione, pronti, differiti) senza consumarli |
| `queue:purge --force` | cancella i messaggi pronti e differiti |

Il DSN può arrivare anche dalla variabile d'ambiente `QUEUE_DSN`; `--strategy=redis|rabbitmq|in-memory` è un controllo
facoltativo sullo schema del DSN. `-v`/`-vv` stampano i log info/debug su stderr. Exit code: `0` successo, `1` errore del
backend, `2` errore d'uso.

Esegui i worker sotto un supervisor con `--max-runtime` e `--memory-limit`: il processo esce in modo pulito e viene
riavviato. Configurazioni pronte: [systemd](examples/deploy/queue-worker.service) e
[Supervisor](examples/deploy/supervisord.conf).

## Esempi

La cartella [`examples/`](examples/) (esclusa dal pacchetto Composer) contiene script eseguibili. Servizi e porte sono
quelli di `docker-compose.yml` (`docker compose up -d --wait`); `REDIS_HOST`, `REDIS_PORT`, `AMQP_HOST`, `AMQP_PORT`,
`AMQP_USER`, `AMQP_PASSWORD` li puntano altrove.

| File | Mostra | Esecuzione |
|---|---|---|
| [01-quick-start.php](examples/01-quick-start.php) | pubblicazione e consumo, JSON e testo libero | `php examples/01-quick-start.php` |
| [02-switch-backend.php](examples/02-switch-backend.php) | lo stesso codice su ogni backend, cambia solo il DTO | `php examples/02-switch-backend.php memory\|redis\|predis\|rabbitmq` |
| [03-worker-bootstrap.php](examples/03-worker-bootstrap.php) | handler per `queue-worker work` | vedi la sezione CLI |
| [04-redis.php](examples/04-redis.php) | riconnessione, nome del consumer, store di idempotenza su Redis, batch, differiti, DLQ | `php examples/04-redis.php [predis]` |
| [05-rabbitmq.php](examples/05-rabbitmq.php) | factory della connessione, scaglioni di retry, prefetch, retry tramite coda di attesa, DLQ | `php examples/05-rabbitmq.php` |
| [06-in-memory-tests.php](examples/06-in-memory-tests.php) | test degli handler con un orologio mosso a mano: differiti, retry, scadenza, DLQ | `php examples/06-in-memory-tests.php` |
| [07-manual-consume.php](examples/07-manual-consume.php) | consumo senza il Worker: ricezione, decodifica manuale, ack/retry/reject | `php examples/07-manual-consume.php memory\|redis\|predis\|rabbitmq` |
| [08-worker-in-code.php](examples/08-worker-in-code.php) | Worker avviato da codice PHP: handler come classe con decodifica JSON manuale, middleware, metriche, segnali, due code | `php examples/08-worker-in-code.php memory\|redis\|predis\|rabbitmq` |

I payload di esempio sono in [`examples/data/`](examples/data/), i file di deploy in [`examples/deploy/`](examples/deploy/).

## Configurazione

`QueueConfig` è identica su ogni backend:

| Opzione | Default | Significato |
|---|---|---|
| `name` | `default` | nome della coda |
| `prefix` | `rq` | prefisso delle chiavi Redis / risorse RabbitMQ |
| `maxTextBytes` | 1 MiB | limite di dimensione del testo, in byte |
| `maxHeadersBytes` | 64 KiB | limite di dimensione degli header |
| `maxAttempts` | 3 | fallimenti dell'handler prima della DLQ |
| `maxDeliveries` | 10 | consegne (crash inclusi) prima della DLQ; almeno `maxAttempts` + 1 |
| `claimIdleMs` | 300000 | Redis: dopo questo tempo di inattività un messaggio non confermato viene tolto a un worker bloccato |
| `dlqMaxLength` | 100000 | Redis: retention approssimativa della DLQ |

Layout su Redis (gli hash tag tengono le chiavi di una coda nello stesso slot del cluster): `rq:{orders}:stream`,
`rq:{orders}:delayed`, `rq:{orders}:dlq`, `rq:{orders}:idem:<id>`; consumer group `queue-bridge`.

Topologia RabbitMQ creata da `setup()`: exchange topic `rq.orders` (routing key = canale), quorum queue `rq.orders` con
`x-delivery-limit`, code di attesa `rq.orders.retry.<ms>`, DLQ classic `rq.orders.dlq`.

## Configurazione consigliata dei server

**Redis**

```
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

Senza AOF un riavvio svuota la coda; con una politica di eviction Redis può cancellare chiavi della coda quando la memoria
scarseggia. Usa utenti ACL limitati alle chiavi della coda (`~rq:*`); se l'ACL vieta `INFO`, passa la versione
esplicitamente (altrimenti la libreria ripiega sul livello base 6.0 e scrive un warning nel log).

**RabbitMQ**

- La libreria dichiara quorum queue: in produzione usa almeno tre nodi.
- `consumer_timeout` (default 30 minuti) chiude il canale quando un messaggio resta non confermato più a lungo: tieni gli
  handler ben al di sotto.
- Heartbeat: un worker PHP sincrono non risponde agli heartbeat mentre un handler è in esecuzione, quindi gli handler lunghi
  fanno chiudere la connessione dal broker. Imposta un heartbeat più lungo dell'handler più lento, disattivalo
  (`heartbeat=0`, il default del DSN) oppure usa `PCNTLHeartbeatSender` di php-amqplib (`ext-pcntl`).

## Garanzie di consegna e limiti

- **At-least-once**: un messaggio viene confermato solo dopo che l'handler è terminato. Un crash tra l'handler e l'ack, un
  errore di rete o un lock scaduto producono un **duplicato**: **gli handler devono essere idempotenti** (oppure usa
  `IdempotencyMiddleware`). Retry e spostamenti in DLQ sono atomici su Redis (Lua) e publish-confirm-poi-ack su RabbitMQ:
  mai persi, al peggio duplicati.
- **Nessuna garanzia di ordinamento**: retry, differiti, messaggi restituiti e recuperati cambiano l'ordine. I messaggi
  arrivano nell'ordine di pubblicazione solo in assenza di questi eventi.
- **Nessun fan-out**: una `QueueConfig` è una coda; ogni messaggio viene elaborato da un solo worker. Per il
  publish/subscribe pubblica su più code.
- **Scalare con i processi**: il worker è a processo singolo e non fa fork. Avvia più processi, anche su più macchine,
  sulla stessa coda; due worker non elaborano mai lo stesso messaggio nello stesso momento.
- **`claimIdleMs` e durata degli handler (Redis)**: un messaggio non confermato per `claimIdleMs` è considerato orfano e
  passato a un altro worker. `claimIdleMs` deve superare l'handler più lento (imponilo con `TimeoutMiddleware`), altrimenti
  un messaggio lento viene elaborato due volte in parallelo.
- **RabbitMQ**: gli orfani tornano appena la connessione si chiude; i limiti sono l'heartbeat e `consumer_timeout` descritti
  sopra. `x-delivery-limit` è uguale a `maxDeliveries`, quindi un messaggio che continua a far bloccare il worker viene
  spostato in DLQ dal broker come ultima difesa.
- **Precisione dei differiti su RabbitMQ**: i ritardi usano code di attesa con TTL a scaglioni fissi (default 1 s, 5 s,
  30 s, 2 min, 10 min, 1 h). Si usa lo scaglione più grande non superiore al ritardo residuo e un messaggio che torna in
  anticipo attende di nuovo: la precisione è di circa uno scaglione (1500 ms viene consegnato tra 1,5 e 3,5 s). Oltre lo
  scaglione più grande il messaggio ci ricircola finché non scade. Scegli ritardi di retry adatti agli scaglioni. Su Redis i
  ritardi sono esatti entro il timeout di ricezione; su InMemory seguono l'orologio.
- **Wire format**: i messaggi sono salvati in un formato versionato; un rolling deploy con worker vecchi e nuovi non perde
  nulla. I formati sconosciuti finiscono in DLQ e non bloccano mai il worker. Vedi [UPGRADE.md](UPGRADE.md).

## Supporto delle versioni di Redis

Il livello viene rilevato una volta da `INFO server`; ogni capability è una strategia scelta alla creazione, mai un
controllo di versione nel percorso critico.

| Livello | Comandi usati | Sotto questo livello |
|---|---|---|
| 6.0 | Streams, consumer group, `XPENDING` esteso, script Lua, sorted set, `SET NX PX` | — |
| 6.2 | `XPENDING ... IDLE`, `XTRIM MINID` | filtro di inattività lato client, nessun trim di sicurezza |
| 7.0 | lag da `XINFO GROUPS` | `lag` = null |
| 8.0 | come la 7.0 | |
| 8.2 | `XACKDEL`, `XTRIM ... ACKED` | `XACK` + `XDEL`, trim con `MINID` |
| 8.4 | `XREADGROUP ... CLAIM`, `DELEX ... IFEQ` | orfani rimessi in coda da uno script Lua, rilascio dei lock in Lua |

Lo stream principale non viene mai accorciato con `MAXLEN` (cancellerebbe messaggi non letti): le entry confermate vengono
cancellate, e il trim è solo una rete di sicurezza che rimuove entry già confermate. La CI esegue i test di integrazione su
Redis 6.0, 6.2, 7.0, 7.4, 8.0, 8.2 e 8.4 con ext-redis e Predis, e su RabbitMQ 3.13 e 4.x, con PHP 8.3, 8.4 e 8.5.

## Benchmark

```bash
docker compose up -d --wait
# argomenti: DSN, messaggi (default 20000), dimensione del batch (default 100); disattiva Xdebug per numeri significativi
php -d xdebug.mode=off tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0' 20000 100
php -d xdebug.mode=off tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0?version=6.0'   # strategie della 6.0
php -d xdebug.mode=off tests/Benchmark/benchmark.php 'amqp://guest:guest@127.0.0.1:35672'
```

Risultati indicativi: PHP 8.3 CLI con OPcache, Xdebug disattivato, AMD Ryzen 9 9900X; server in Docker sulla stessa
macchina, raggiunti tramite le porte pubblicate (Redis 8.4, RabbitMQ 4.x a nodo singolo); payload JSON di 157 byte, batch
da 100, un processo. "consume" comprende la ricezione, un handler vuoto e l'ack.

| Backend | publish | publishBatch | consume |
|---|---:|---:|---:|
| InMemory | 300.000 msg/s | 327.000 msg/s | 68.000 msg/s |
| Redis, ext-redis | 3.000 msg/s | 115.000 msg/s | 30.600 msg/s |
| Redis, Predis | 3.400 msg/s | 95.500 msg/s | 32.000 msg/s |
| RabbitMQ (confirm attivi) | 1.100 msg/s | 25.000 msg/s | 6.000 msg/s |

Livelli Redis forzati sullo stesso server 8.4 (ext-redis):

| Livello | publishBatch | consume |
|---|---:|---:|
| 6.0 | 107.000 msg/s | 32.200 msg/s |
| 6.2 | 110.000 msg/s | 29.400 msg/s |
| 7.0 | 106.000 msg/s | 30.000 msg/s |
| 8.0 | 110.000 msg/s | 30.700 msg/s |
| 8.2 | 79.000 msg/s | 27.400 msg/s |
| 8.4 | 108.000 msg/s | 33.300 msg/s |

Un singolo `publish()` è limitato dal round trip di rete (uno per messaggio, più il publisher confirm su RabbitMQ; qui
anche dal port forwarding di Docker): usa `publishBatch()` per il throughput. Il consumo cresce con il numero di processi
worker. I livelli differiscono nell'atomicità e nei round trip delle operazioni di manutenzione (recupero degli orfani,
trim), non nel percorso critico, quindi i loro numeri rientrano nella variabilità tra un'esecuzione e l'altra.

## Estendere

Un nuovo backend (per esempio SQS) implementa `StrategyDtoInterface`, `TransportInterface` (più le interfacce di
capability opzionali che supporta: setup, manutenzione, statistiche, dead letter, ack in batch, svuotamento, elenco) e `TransportBuilderInterface`, e viene registrato nel factory:

```php
$factory = new QueueFactory(builders: [new SqsTransportBuilder(), new RedisTransportBuilder(), new InMemoryTransportBuilder()]);
```

Il core non cambia. `EnvelopeCodec` converte i messaggi nel wire format versionato, e la classe astratta
`TransportContractTestCase` nel repository (`tests/Contract`) verifica la stessa semantica dei backend forniti.

Un client personalizzato (wrapper aziendale, facade di un framework) implementa `RedisClientInterface` o
`AmqpConnectorInterface` e viene passato al costruttore del DTO.

## Sviluppo

```bash
composer qa                      # validate, Rector, php-cs-fixer, PHPStan max + regole architetturali, coverage 100%
docker compose up -d --wait      # Redis su 16379, RabbitMQ su 35672 (management su 35673)
REDIS_DSN=redis://127.0.0.1:16379/15 AMQP_DSN=amqp://guest:guest@127.0.0.1:35672 composer test:integration
composer mutation                # Infection, MSI >= 85%
```

Vedi [CHANGELOG.md](CHANGELOG.md) e [UPGRADE.md](UPGRADE.md).

## Licenza

GPL-2.0-only.
