<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis;

use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Message\MessageId;

/**
 * Chiavi di una coda. Il nome della coda è sempre tra hash tag: tutte le chiavi cadono
 * nello stesso slot di Redis Cluster, requisito degli script Lua multi-key.
 *
 * @internal
 */
final readonly class RedisKeys
{
    public const string GROUP = 'queue-bridge';

    private string $base;

    /**
     * Complessità: O(1).
     */
    public function __construct(QueueConfig $queueConfig)
    {
        $this->base = \sprintf('%s:{%s}', $queueConfig->prefix, $queueConfig->name);
    }

    /**
     * Complessità: O(1).
     */
    public function stream(): string
    {
        return $this->base.':stream';
    }

    /**
     * Complessità: O(1).
     */
    public function delayed(): string
    {
        return $this->base.':delayed';
    }

    /**
     * Complessità: O(1).
     */
    public function deadLetters(): string
    {
        return $this->base.':dlq';
    }

    /**
     * Complessità: O(1).
     */
    public function idempotency(MessageId $messageId): string
    {
        return $this->base.':idem:'.$messageId->value;
    }

    /**
     * Complessità: O(1).
     */
    public function group(): string
    {
        return self::GROUP;
    }
}
