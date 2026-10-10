<?php

declare(strict_types=1);

namespace snipershady\Queue\Factory;

/**
 * Backend, connessione e opzioni del backend in un solo oggetto tipizzato. Per cambiare
 * backend si sostituisce solo il DTO passato a QueueFactoryInterface::create().
 *
 * @api
 */
interface StrategyDtoInterface
{
    /**
     * Nome leggibile per log e diagnostica (es. "redis").
     */
    public function name(): string;
}
