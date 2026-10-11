<?php

declare(strict_types=1);

namespace QueueBridge\System;

/**
 * Notifica le richieste di terminazione del processo (SIGTERM/SIGINT) per il graceful shutdown.
 *
 * @api
 */
interface SignalHandlerInterface
{
    /**
     * Complessità: O(1).
     *
     * @param callable(int): void $onTerminate riceve il numero del segnale
     */
    public function onTerminate(callable $onTerminate): void;

    /**
     * Ripristina il comportamento di default dei segnali.
     *
     * Complessità: O(1).
     */
    public function reset(): void;
}
