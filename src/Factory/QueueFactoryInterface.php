<?php

declare(strict_types=1);

namespace snipershady\Queue\Factory;

use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\QueueInterface;

/**
 * @api
 */
interface QueueFactoryInterface
{
    /**
     * @throws UnsupportedStrategyException se nessun builder accetta il DTO
     * @throws BackendException             se il backend non è raggiungibile o non è compatibile
     */
    public function create(StrategyDtoInterface $strategy, QueueConfig $config = new QueueConfig()): QueueInterface;
}
