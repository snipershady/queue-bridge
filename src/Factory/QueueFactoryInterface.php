<?php

declare(strict_types=1);

namespace QueueBridge\Factory;

use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\BackendException;
use QueueBridge\Exception\UnsupportedStrategyException;
use QueueBridge\QueueInterface;

/**
 * @api
 */
interface QueueFactoryInterface
{
    /**
     * Complessità: O(b) + il costo del builder scelto, b = numero di builder.
     *
     * @throws UnsupportedStrategyException se nessun builder accetta il DTO
     * @throws BackendException             se il backend non è raggiungibile o non è compatibile
     */
    public function create(StrategyDtoInterface $strategy, QueueConfig $config = new QueueConfig()): QueueInterface;
}
