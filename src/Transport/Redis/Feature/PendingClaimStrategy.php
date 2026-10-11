<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisKeys;

/**
 * Redis >= 6.0: `XPENDING key group - + count`, inattività filtrata lato client.
 *
 * @internal
 */
final readonly class PendingClaimStrategy implements IdleClaimStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function claimsOnRead(): bool
    {
        return false;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function readArguments(int $minIdleMs): array
    {
        return [];
    }

    /**
     * Complessità: O(limit), un round trip (filtro di inattività lato client).
     */
    #[Override]
    public function findIdle(RedisClientInterface $redisClient, RedisKeys $redisKeys, int $minIdleMs, int $limit): array
    {
        $pending = $redisClient->execute(['XPENDING', $redisKeys->stream(), $redisKeys->group(), '-', '+', (string) $limit]);

        return PendingEntries::idleIds($pending, $minIdleMs);
    }
}
