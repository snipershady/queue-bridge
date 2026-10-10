<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use Override;

/**
 * @api
 */
final readonly class NativeMemoryMeter implements MemoryMeterInterface
{
    #[Override]
    public function usageBytes(): int
    {
        return memory_get_usage(real_usage: true);
    }

    #[Override]
    public function collectCycles(): int
    {
        return gc_collect_cycles();
    }
}
