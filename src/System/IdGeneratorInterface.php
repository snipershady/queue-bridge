<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use snipershady\Queue\Message\MessageId;

/**
 * @api
 */
interface IdGeneratorInterface
{
    /**
     * Restituisce un ID nuovo, strettamente crescente rispetto ai precedenti generati dalla stessa istanza.
     *
     * Complessità: O(1) nel generatore fornito.
     */
    public function generate(): MessageId;
}
