<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use InvalidArgumentException;

/**
 * Allarme a tempo per interrompere codice sincrono (TimeoutMiddleware). Un solo allarme attivo alla volta.
 *
 * @api
 */
interface AlarmInterface
{
    /**
     * Indica se l'allarme può davvero interrompere l'esecuzione.
     *
     * Complessità: O(1).
     */
    public function isSupported(): bool;

    /**
     * Programma $onAlarm dopo $seconds secondi, sostituendo l'allarme precedente.
     *
     * Complessità: O(1).
     *
     * @param callable(): void $onAlarm
     *
     * @throws InvalidArgumentException se $seconds < 1
     */
    public function schedule(int $seconds, callable $onAlarm): void;

    /**
     * Complessità: O(1).
     */
    public function cancel(): void;
}
