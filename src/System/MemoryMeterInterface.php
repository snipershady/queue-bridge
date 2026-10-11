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
     *
     * Complessità: O(1).
     */
    public function usageBytes(): int;

    /**
     * Forza la raccolta dei cicli; restituisce il numero di cicli liberati.
     *
     * Complessità: O(r), r = radici del garbage collector (quella di `gc_collect_cycles()`).
     */
    public function collectCycles(): int;
}
