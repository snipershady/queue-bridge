<?php

declare(strict_types=1);

namespace QueueBridge\System;

use Override;

/**
 * @api
 */
final readonly class NativeMemoryMeter implements MemoryMeterInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function usageBytes(): int
    {
        return memory_get_usage(real_usage: true);
    }

    /**
     * Complessità: O(r), r = radici del garbage collector (quella di `gc_collect_cycles()`).
     */
    #[Override]
    public function collectCycles(): int
    {
        return gc_collect_cycles();
    }
}
