<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis;

use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Message\MessageId;

/**
 * Chiavi di una coda (CLAUDE.md §8.3). Il nome della coda è sempre tra hash tag: tutte le chiavi cadono
 * nello stesso slot di Redis Cluster, requisito degli script Lua multi-key.
 *
 * @internal
 */
final readonly class RedisKeys
{
    public const string GROUP = 'queue-bridge';

    private string $base;

    public function __construct(QueueConfig $queueConfig)
    {
        $this->base = \sprintf('%s:{%s}', $queueConfig->prefix, $queueConfig->name);
    }

    public function stream(): string
    {
        return $this->base.':stream';
    }

    public function delayed(): string
    {
        return $this->base.':delayed';
    }

    public function deadLetters(): string
    {
        return $this->base.':dlq';
    }

    public function idempotency(MessageId $messageId): string
    {
        return $this->base.':idem:'.$messageId->value;
    }

    public function group(): string
    {
        return self::GROUP;
    }
}
