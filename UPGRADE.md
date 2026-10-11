# Upgrade guide

Migration steps between major versions, for applications that integrate the library.

## Compatibility promise

- The library follows [Semantic Versioning](https://semver.org/). Classes and interfaces marked `@api` change only in a
  major version; anything marked `@internal` (Redis strategies and scripts, RabbitMQ topology, CLI internals, DSN parsing)
  may change in a minor version and must not be used directly.
- An API is marked `@deprecated`, with its replacement, at least one minor version before it is removed in the next major.
  Every deprecation is listed in [CHANGELOG.md](CHANGELOG.md).
- **Wire format**: every major reads at least the message format written by the previous major, so messages already in a
  queue survive the upgrade and old and new workers can run side by side during a rolling deploy.
- **Storage layout** (Redis keys, RabbitMQ exchanges and queues): a change is a breaking change and comes with a documented
  migration, or with code that still reads the previous layout.

## Checklist for every major upgrade

1. Read the section of the target version below and the `Removed` / `Changed` entries of the CHANGELOG.
2. Fix the deprecations reported on the latest minor of your current major first (PHPStan with
   `phpstan-deprecation-rules`, or the `@deprecated` tags in your IDE).
3. Deploy the new version to the **producers and workers together, or workers first**: new workers read the old wire format,
   old workers may not read the new one.
4. Run `setup()` once (or `queue-worker work`, which calls it) if the section mentions topology changes.
5. Check the dead letter queue (`queue-worker dlq:list`) after the deploy.

## 1.0

First stable release: nothing to migrate.

- Wire format version: `1`.
- Redis layout: `{prefix}:{<queue>}:stream`, `{prefix}:{<queue>}:delayed`, `{prefix}:{<queue>}:dlq`,
  `{prefix}:{<queue>}:idem:<id>`, consumer group `queue-bridge`.
- RabbitMQ topology: topic exchange `{prefix}.{queue}`, quorum queue `{prefix}.{queue}` with `x-delivery-limit`, wait queues
  `{prefix}.{queue}.retry.<ms>`, classic DLQ `{prefix}.{queue}.dlq`.

If you wrote your own `IdempotencyStoreInterface` against a pre-release snapshot: `acquire()` now returns an
`AcquireResult` instead of `?LockToken`. Return `AcquireResult::acquired($token)`, `AcquireResult::completed()` when the
message was already processed, or `AcquireResult::inProgress($remainingLockMs)` when another worker holds the lock, and
decide among them atomically.

Changing `QueueConfig::$maxDeliveries` on RabbitMQ after the queue exists requires deleting and recreating the main queue
(queue arguments cannot be changed in place): drain it first.
