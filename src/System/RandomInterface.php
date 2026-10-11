<?php

declare(strict_types=1);

namespace QueueBridge\System;

use InvalidArgumentException;

/**
 * Sorgente di byte casuali crittograficamente sicuri.
 *
 * @api
 */
interface RandomInterface
{
    /**
     * Complessità: O(l), l = numero di byte.
     *
     * @throws InvalidArgumentException se $length < 1
     *
     * @return non-empty-string esattamente $length byte
     */
    public function bytes(int $length): string;
}
