<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;

/**
 * Redis < 7.0: XINFO GROUPS non riporta il lag.
 *
 * @internal
 */
final readonly class NullLagStrategy implements LagStrategyInterface
{
    #[Override]
    public function lag(array $groupInfo): ?int
    {
        return null;
    }
}
