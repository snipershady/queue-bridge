<?php

declare(strict_types=1);

namespace QueueBridge\Factory;

use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\BackendException;
use QueueBridge\Exception\UnsupportedStrategyException;
use QueueBridge\Transport\TransportInterface;

/**
 * Crea il transport per un tipo di DTO. Per un backend nuovo: un DTO, un TransportInterface e un builder,
 * registrato con `new QueueFactory(builders: [...])`.
 *
 * @api
 */
interface TransportBuilderInterface
{
    /**
     * Complessità: O(1).
     */
    public function supports(StrategyDtoInterface $strategy): bool;

    /**
     * Complessità: O(1) nei builder forniti, più un round trip su Redis per leggere la versione.
     *
     * @throws UnsupportedStrategyException se il DTO non è supportato
     * @throws BackendException             se il backend non è raggiungibile o non è compatibile
     */
    public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface;
}
