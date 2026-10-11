<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

/**
 * Lag del consumer group (messaggi mai letti), se il server lo fornisce.
 *
 * @internal
 */
interface LagStrategyInterface
{
    /**
     * Complessità: O(1).
     *
     * @param array<string, mixed> $groupInfo voce del gruppo in XINFO GROUPS
     */
    public function lag(array $groupInfo): ?int;
}
