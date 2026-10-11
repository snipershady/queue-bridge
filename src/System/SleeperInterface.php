<?php

declare(strict_types=1);

namespace QueueBridge\System;

use InvalidArgumentException;

/**
 * @api
 */
interface SleeperInterface
{
    /**
     * Complessità: O(1) di calcolo; blocca il processo per $milliseconds.
     *
     * @throws InvalidArgumentException se $milliseconds è negativo
     */
    public function sleepMs(int $milliseconds): void;
}
