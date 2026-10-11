<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisKeys;

/**
 * Come si trovano i messaggi orfani (consegnati e non confermati da almeno `claimIdleMs`).
 *
 * @internal
 */
interface IdleClaimStrategyInterface
{
    /**
     * true se gli orfani vengono recuperati direttamente da XREADGROUP (nessuna scansione in maintain()).
     *
     * Complessità: O(1).
     */
    public function claimsOnRead(): bool;

    /**
     * Argomenti aggiuntivi di XREADGROUP, prima di STREAMS.
     *
     * Complessità: O(1).
     *
     * @return list<string>
     */
    public function readArguments(int $minIdleMs): array;

    /**
     * ID delle entry pendenti inattive da almeno $minIdleMs, al più $limit.
     *
     * Complessità: O(limit), al più un round trip.
     *
     * @throws CommandFailedException
     *
     * @return list<string>
     */
    public function findIdle(RedisClientInterface $redisClient, RedisKeys $redisKeys, int $minIdleMs, int $limit): array;
}
