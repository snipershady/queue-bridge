<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use Override;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\RedisKeys;

/**
 * Redis 6.0: nessun trim sicuro disponibile (MAXLEN perderebbe messaggi).
 *
 * @internal
 */
final readonly class NoTrimStrategy implements TrimStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function trim(RedisClientInterface $redisClient, RedisKeys $redisKeys): int
    {
        return 0;
    }
}
