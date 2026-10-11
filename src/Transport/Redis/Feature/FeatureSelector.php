<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use snipershady\Queue\Transport\Redis\Version\RedisCapabilities;

/**
 * Sceglie una strategia per ogni punto di variazione, una sola volta alla creazione.
 *
 * @internal
 */
final class FeatureSelector
{
    /**
     * Complessità: O(1).
     */
    public static function select(RedisCapabilities $redisCapabilities): RedisFeatures
    {
        return new RedisFeatures(
            capabilities: $redisCapabilities,
            idleClaim: match (true) {
                $redisCapabilities->readGroupClaim => new ReadGroupClaimStrategy(),
                $redisCapabilities->xpendingIdle => new PendingIdleStrategy(),
                default => new PendingClaimStrategy(),
            },
            ack: $redisCapabilities->xackdel ? new AckDelStrategy() : new AckThenDeleteStrategy(),
            trim: match (true) {
                $redisCapabilities->trimAcked => new AckedTrimStrategy(),
                $redisCapabilities->xtrimMinId => new MinIdTrimStrategy(),
                default => new NoTrimStrategy(),
            },
            lag: $redisCapabilities->groupLag ? new GroupInfoLagStrategy() : new NullLagStrategy(),
            lockRelease: $redisCapabilities->delex ? new DelexLockRelease() : new LuaLockRelease(),
        );
    }
}
