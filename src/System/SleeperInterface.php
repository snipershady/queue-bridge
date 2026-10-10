<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use InvalidArgumentException;

/**
 * @api
 */
interface SleeperInterface
{
    /**
     * @throws InvalidArgumentException se $milliseconds è negativo
     */
    public function sleepMs(int $milliseconds): void;
}
