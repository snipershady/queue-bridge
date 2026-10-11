<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;

/**
 * Redis >= 8.4: `DELEX key IFEQ token`.
 *
 * @internal
 */
final readonly class DelexLockRelease implements LockReleaseStrategyInterface
{
    /**
     * Complessità: O(1), un round trip.
     */
    #[Override]
    public function release(RedisClientInterface $redisClient, string $key, string $token): void
    {
        $redisClient->execute(['DELEX', $key, 'IFEQ', $token]);
    }
}
