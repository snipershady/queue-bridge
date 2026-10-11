<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use snipershady\Queue\Exception\BackendException;

/**
 * @api
 */
interface SetupAwareInterface
{
    /**
     * Crea le risorse del backend (stream, gruppi, code, exchange). Idempotente.
     *
     * Complessità: O(1) su Redis e InMemory, O(T) dichiarazioni su RabbitMQ (T = scaglioni).
     *
     * @throws BackendException
     */
    public function setup(): void;
}
