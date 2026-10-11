<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisKeys;

/**
 * Redis >= 8.4: `XREADGROUP ... CLAIM min-idle-time` recupera gli orfani in lettura, in un solo round trip.
 *
 * @internal
 */
final readonly class ReadGroupClaimStrategy implements IdleClaimStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function claimsOnRead(): bool
    {
        return true;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function readArguments(int $minIdleMs): array
    {
        return ['CLAIM', (string) $minIdleMs];
    }

    /**
     * Complessità: O(1), nessun round trip (gli orfani li recupera XREADGROUP ... CLAIM).
     */
    #[Override]
    public function findIdle(RedisClientInterface $redisClient, RedisKeys $redisKeys, int $minIdleMs, int $limit): array
    {
        return [];
    }
}
