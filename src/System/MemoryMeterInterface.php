<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

/**
 * @api
 */
interface MemoryMeterInterface
{
    /**
     * Memoria allocata dal processo, in byte.
     */
    public function usageBytes(): int;

    /**
     * Forza la raccolta dei cicli; restituisce il numero di cicli liberati.
     */
    public function collectCycles(): int;
}
