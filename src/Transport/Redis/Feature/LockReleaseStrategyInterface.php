<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;

/**
 * Cancella una chiave solo se contiene ancora $token (confronto atomico).
 *
 * @internal
 */
interface LockReleaseStrategyInterface
{
    /**
     * Complessità: O(1), un round trip.
     *
     * @throws CommandFailedException
     */
    public function release(RedisClientInterface $redisClient, string $key, string $token): void;
}
