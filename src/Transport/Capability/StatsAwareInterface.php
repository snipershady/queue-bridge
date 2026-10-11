<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Transport\QueueStats;

/**
 * @api
 */
interface StatsAwareInterface
{
    /**
     * Complessità: Un numero costante di round trip su Redis, O(T) su RabbitMQ (T = scaglioni).
     *
     * @throws BackendException
     */
    public function stats(): QueueStats;
}
