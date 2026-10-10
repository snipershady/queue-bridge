<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use InvalidArgumentException;

/**
 * Sorgente di byte casuali crittograficamente sicuri.
 *
 * @api
 */
interface RandomInterface
{
    /**
     * @throws InvalidArgumentException se $length < 1
     *
     * @return non-empty-string esattamente $length byte
     */
    public function bytes(int $length): string;
}
