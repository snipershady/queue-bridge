<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Transport\Redis\RedisReply;

/**
 * Redis >= 7.0: campo `lag` di XINFO GROUPS (nil se il server non riesce a calcolarlo).
 *
 * @internal
 */
final readonly class GroupInfoLagStrategy implements LagStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function lag(array $groupInfo): ?int
    {
        $lag = $groupInfo['lag'] ?? null;

        return null === $lag ? null : RedisReply::int($lag, 'XINFO GROUPS lag');
    }
}
