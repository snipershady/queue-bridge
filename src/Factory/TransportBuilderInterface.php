<?php

declare(strict_types=1);

namespace snipershady\Queue\Factory;

use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Transport\TransportInterface;

/**
 * Crea il transport per un tipo di DTO. Per un backend nuovo: un DTO, un TransportInterface e un builder,
 * registrato con `new QueueFactory(builders: [...])` (CLAUDE.md §6.2).
 *
 * @api
 */
interface TransportBuilderInterface
{
    public function supports(StrategyDtoInterface $strategy): bool;

    /**
     * @throws UnsupportedStrategyException se il DTO non è supportato
     * @throws BackendException             se il backend non è raggiungibile o non è compatibile
     */
    public function build(StrategyDtoInterface $strategy, QueueConfig $config): TransportInterface;
}
