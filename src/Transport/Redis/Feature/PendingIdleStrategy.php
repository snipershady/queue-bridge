<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use Override;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\RedisKeys;

/**
 * Redis >= 6.2: `XPENDING key group IDLE ms - + count`, inattività filtrata lato server.
 *
 * Non si usa XAUTOCLAIM: assegnerebbe gli orfani al consumer che esegue la manutenzione, mentre devono
 * tornare disponibili a tutti (vengono rimessi in coda da RedisScripts::requeueOrphans()).
 *
 * @internal
 */
final readonly class PendingIdleStrategy implements IdleClaimStrategyInterface
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
     * Complessità: O(limit), un round trip (filtro di inattività lato server).
     */
    #[Override]
    public function findIdle(RedisClientInterface $redisClient, RedisKeys $redisKeys, int $minIdleMs, int $limit): array
    {
        $pending = $redisClient->execute(['XPENDING', $redisKeys->stream(), $redisKeys->group(), 'IDLE', (string) $minIdleMs, '-', '+', (string) $limit]);

        return PendingEntries::idleIds($pending, $minIdleMs);
    }
}
