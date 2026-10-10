<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Support;

use Override;
use snipershady\Queue\System\MemoryMeterInterface;

final class FakeMemoryMeter implements MemoryMeterInterface
{
    public int $collections = 0;

    public function __construct(public int $usage = 0)
    {
    }

    #[Override]
    public function usageBytes(): int
    {
        return $this->usage;
    }

    #[Override]
    public function collectCycles(): int
    {
        ++$this->collections;

        return 0;
    }
}
