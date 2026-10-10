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
     * @throws BackendException
     */
    public function stats(): QueueStats;
}
