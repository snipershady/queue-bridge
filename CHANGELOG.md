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
