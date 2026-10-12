<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use Override;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\RedisKeys;

/**
 * Redis >= 6.0: `XPENDING key group - + count`, inattività filtrata lato client.
 *
 * Basta la prima pagina della PEL: XREADGROUP consegna le entry in ordine di ID e la libreria non riassegna mai
 * un'entry (gli orfani vengono copiati in entry nuove), quindi un ID più basso è stato consegnato prima ed è
 * inattivo almeno quanto quelli successivi. Se la testa della PEL non è inattiva, non lo è nemmeno il resto.
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
