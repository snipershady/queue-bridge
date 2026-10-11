# Changelog

Tutte le modifiche rilevanti del progetto sono documentate in questo file.

Il formato segue [Keep a Changelog](https://keepachangelog.com/it-IT/1.1.0/) e il progetto aderisce al [Semantic Versioning](https://semver.org/lang/it/).

## [Unreleased]

## [1.0.0] - 2026-10-10

Prima release stabile.

### Added

- Scaffold del progetto: toolchain di qualità (Rector, php-cs-fixer, PHPStan `max` con phpat, type-coverage e dead-code-detector, PHPUnit), gate di coverage al 100% su metodi, istruzioni e branch, mutation testing con Infection, CI su PHP 8.3–8.5 (8.6 sperimentale) e `docker-compose.yml` per Redis e RabbitMQ.
- `QueueException` (astratta), radice della gerarchia di eccezioni, e `SerializationException`.
- `System/`: `SystemClock` (PSR-20, UTC), `NativeSleeper`, `NativeRandom`, `UuidV7Generator` (UUIDv7 monotono, RFC 9562), `NativeMemoryMeter`, gestione di SIGTERM/SIGINT (`PcntlSignalHandler`, `NullSignalHandler`) e allarmi SIGALRM (`PcntlAlarm`, `NullAlarm`), tutti dietro interfacce iniettabili.
- `Message/`: `MessageId`, `Message`, `Delivery`, `OutgoingMessage`, `Envelope`, `FailureKind`, `FailureReason`, con validazione di canale, UTF-8 e header.
- `Text/`: `TextDecoder` che restituisce JSON decodificato se valido, altrimenti il testo grezzo, senza mai lanciare eccezioni sul contenuto.
- `Codec/`: wire format versionato (v1) con `EnvelopeCodec`.
- `Config/QueueConfig` con validazione dei limiti (incluso `maxDeliveries >= maxAttempts + 1`).
- Contratto del transport: `TransportInterface` (con `release()` e `ping()`), interfacce opzionali `SetupAwareInterface`, `MaintenanceAwareInterface`, `StatsAwareInterface`, `DeadLetterAwareInterface`, `BatchAckAwareInterface`, e i value object `TransportCapabilities`, `QueueStats`, `MaintenanceReport`, `DeadLetter`.
- `BackendException` (astratta), `ConnectionException`, `BatchSendException`.
- `InMemoryTransport`, implementazione di riferimento del contratto, con consumer concorrenti (`newConsumer()`), differiti, orfani, DLQ e statistiche.
- `TransportContractTestCase`: scenari comuni a tutti i backend.
- `Producer` (limiti in byte, ID UUIDv7, scadenza, pipeline di middleware) con `CorrelationIdMiddleware` (`correlation_id`/`causation_id` tramite `CorrelationContext`) e `TraceContextMiddleware` (W3C `traceparent`).
- `Worker`: registry degli handler con fallback `*`, middleware, retry con `RetryStrategyInterface` (`ExponentialBackoff`, `FixedDelay`, `NoRetry`), DLQ, `maxDeliveries` contro i messaggi che fanno crashare il worker, scadenza, politiche per canali sconosciuti e messaggi scaduti, ack in batch, manutenzione periodica, limiti di messaggi/tempo/memoria, graceful shutdown con `release()` (anche da codice con `WorkerInterface::stop()`), eventi del ciclo di vita. Su Redis e InMemory il worker restituisce con `release()` i messaggi di un batch ricevuti da troppo tempo per finire prima di `claimIdleMs`, così un altro worker non li riprende mentre aspettano il proprio turno.
- Middleware del worker: `LoggingMiddleware`, `TimeoutMiddleware`, `IdempotencyMiddleware` (con `IdempotencyStoreInterface` e `InMemoryIdempotencyStore`), `CorrelationContextMiddleware`. `IdempotencyStoreInterface::acquire()` restituisce un `AcquireResult` (`AcquireStatus::Acquired`, `InProgress`, `Completed`): un duplicato in elaborazione presso un altro worker viene ritentato allo scadere del lock, mai confermato, così non va perso se quel worker fallisce o muore.
- `RetryableException`, `UnrecoverableMessageException`, `UnknownChannelException`.
- Backend Redis (>= 6.0, Streams): `RedisTransport` con script Lua atomici e controllo di proprietà delle consegne, orfani rimessi in coda con il conteggio delle consegne, differiti in sorted set, DLQ con motivo del fallimento, trim di sicurezza che non perde messaggi, statistiche; `RedisIdempotencyStore` (acquisizione del lock in uno script Lua che distingue "completato" da "in elaborazione").
- Rilevamento della versione Redis (`VersionDetector`, `RedisVersionEnum` GE60 → GE84) e strategie per capability scelte da `FeatureSelector` (`XPENDING IDLE`, `XACKDEL`, `XTRIM ACKED`, `XREADGROUP CLAIM`, `DELEX`).
- Adapter `PhpRedisClient` (ext-redis, riconnessione tramite factory) e `PredisClient` dietro `RedisClientInterface`.
- `CommandFailedException`, `VersionMismatchException`.
- Backend RabbitMQ (AMQP 0-9-1, quorum queue): `RabbitMqTransport` con publisher confirms, ricevute legate al canale, recupero nativo degli orfani, differiti e retry tramite code di attesa a scaglioni (`RetryTiers`, header `x-qb-due-at`), DLQ classic ispezionabile e riaccodabile, `x-delivery-limit` con dead-letter at-least-once come rete di sicurezza, statistiche; `Topology`.
- `AmqpChannelInterface` e `AmqpConnectorInterface` con gli adapter `PhpAmqpLibChannel` e `PhpAmqpLibConnector` (riapertura del canale dopo ogni errore, riconnessione tramite factory).
- `QueueFactory` con dispatch dei DTO ai builder registrati, `QueueInterface`/`Queue` (producer, worker, setup, ping, stats, DLQ) e `QueueBridge::factory()` con i builder di InMemory, Redis e RabbitMQ: si cambia backend sostituendo solo il DTO.
- DTO `StrategyDtoInMemory`, `StrategyDtoRedis` (`fromPhpRedis`, `fromPredis`), `StrategyDtoRabbitMq` (`fromPhpAmqpLib`, `fromConnection`, `fromConnectionFactory`) e i builder `InMemoryTransportBuilder`, `RedisTransportBuilder` (rilevamento e verifica della versione), `RabbitMqTransportBuilder`.
- `RedisIdempotencyStore::create()`, `UnsupportedStrategyException`.
- `MultiQueueWorker`: consuma più code a turno nello stesso processo, con limiti, `stopWhenEmpty` e graceful shutdown comuni; lo stop viene inoltrato a ogni `WorkerInterface`, anche alle implementazioni custom (decoratori, test double).
- `Dsn/`: `DsnParser`, `Dsn`, `StrategyEnum` e `StrategyDtoFactory::fromDsn()` per `redis://`, `rediss://`, `amqp://`, `amqps://` e `memory://`, con opzioni in query string (client, versione, consumer, prefetch, confirms, scaglioni, heartbeat, timeout).
- CLI `bin/queue-worker` scritta senza dipendenze: `work` (handler da file di bootstrap), `stats`, `ping`, `dlq:list`, `dlq:requeue`, `dlq:purge --force`; DSN da `--dsn` o `QUEUE_DSN`, output JSON, log su stderr, exit code 0/1/2.
- Svuotamento ed elenco della coda: interfacce opzionali `PurgeAwareInterface::purge()` (pronti e differiti; in elaborazione e DLQ restano) e `BrowseAwareInterface::browse()` (`QueuedMessage` con stato `InFlight`/`Ready`/`Delayed`, senza consumare), implementate da InMemory, Redis (script Lua a pagine che saltano le entry nella PEL) e RabbitMQ (`queue.purge` su coda principale e code di attesa; `basic.get` + requeue per l'elenco, solo messaggi pronti); `QueueInterface::purge()`/`browse()` e comandi CLI `queue:list` e `queue:purge --force`.
- `DOC.md` (in inglese, traduzione in `DOC_IT.md`): introduzione alle code con esempi d'uso della libreria, diagrammi UML in Mermaid (classi, sequenza, attività), specifiche, concorrenza e schedulazione del worker.
- `Cli\CommandLine::arguments()`: argomenti della riga di comando letti da `$_SERVER['argv']` con `filter_var()`, indipendenti da `register_argc_argv` nel php.ini; usato da `bin/queue-worker`, esempi e benchmark.
- Benchmark `tests/Benchmark/benchmark.php` (publish, publish in batch e consume in msg/s per backend e livello Redis) con risultati nel README.
- Esempi in `examples/`: avvio rapido, cambio di backend sostituendo solo il DTO, bootstrap per la CLI e un esempio per ogni backend: Redis (riconnessione, nome del consumer, store di idempotenza su Redis, differiti, DLQ), RabbitMQ (scaglioni di retry, prefetch, retry tramite coda di attesa), InMemory nei test con un orologio manuale, consumo senza Worker con decodifica manuale, Worker avviato da codice PHP con `MultiQueueWorker`; payload di esempio in `examples/data/` e configurazioni systemd e Supervisor in `examples/deploy/`.
- README in inglese con confronto tra Redis, RabbitMQ e InMemory (vantaggi e svantaggi), integrazione per backend, consumo con e senza Worker, CLI, configurazione consigliata dei server, garanzie e limiti, versioni Redis, benchmark, estensione; traduzione italiana in `README_IT.md`.
- `UPGRADE.md` con la politica di compatibilità (API `@api`/`@internal`, deprecazioni, wire format, layout di chiavi e topologia).
- CI: test unitari su PHP 8.3, 8.4, 8.5 (8.6 sperimentale), integrazione su Redis 6.0, 6.2, 7.0, 7.4, 8.0, 8.2, 8.4 e RabbitMQ 3.13, 4.x per ogni versione di PHP, job end-to-end (cambio di backend, esempi, CLI, benchmark) con Redis 6.0 e 8.4; mutation testing bloccante (MSI >= 85%).
- API pubblica marcata `@api`; strategie Redis, script Lua, topologia RabbitMQ, parsing dei DSN, interni di CLI e Worker marcati `@internal`.

[Unreleased]: https://github.com/snipershady/queue-bridge/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/snipershady/queue-bridge/releases/tag/v1.0.0
