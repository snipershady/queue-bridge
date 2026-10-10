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
     * @throws BackendException
     */
    public function setup(): void;
}
