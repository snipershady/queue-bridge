<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Capability;

use QueueBridge\Exception\BackendException;
use QueueBridge\Transport\QueueStats;

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
