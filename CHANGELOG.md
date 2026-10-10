# Changelog

Tutte le modifiche rilevanti del progetto sono documentate in questo file.

Il formato segue [Keep a Changelog](https://keepachangelog.com/it-IT/1.1.0/) e il progetto aderisce al [Semantic Versioning](https://semver.org/lang/it/).

## [Unreleased]

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
- `Worker`: registry degli handler con fallback `*`, middleware, retry con `RetryStrategyInterface` (`ExponentialBackoff`, `FixedDelay`, `NoRetry`), DLQ, `maxDeliveries` contro i messaggi che fanno crashare il worker, scadenza, politiche per canali sconosciuti e messaggi scaduti, ack in batch, manutenzione periodica, limiti di messaggi/tempo/memoria, graceful shutdown con `release()`, eventi del ciclo di vita.
- Middleware del worker: `LoggingMiddleware`, `TimeoutMiddleware`, `IdempotencyMiddleware` (con `IdempotencyStoreInterface` e `InMemoryIdempotencyStore`), `CorrelationContextMiddleware`.
- `RetryableException`, `UnrecoverableMessageException`, `UnknownChannelException`.
- Backend Redis (>= 6.0, Streams): `RedisTransport` con script Lua atomici e controllo di proprietà delle consegne, orfani rimessi in coda con il conteggio delle consegne, differiti in sorted set, DLQ con motivo del fallimento, trim di sicurezza che non perde messaggi, statistiche; `RedisIdempotencyStore`.
- Rilevamento della versione Redis (`VersionDetector`, `RedisVersionEnum` GE60 → GE84) e strategie per capability scelte da `FeatureSelector` (`XPENDING IDLE`, `XACKDEL`, `XTRIM ACKED`, `XREADGROUP CLAIM`, `DELEX`).
- Adapter `PhpRedisClient` (ext-redis, riconnessione tramite factory) e `PredisClient` dietro `RedisClientInterface`.
- `CommandFailedException`, `VersionMismatchException`.
