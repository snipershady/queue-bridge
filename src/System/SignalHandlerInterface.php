<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

/**
 * Notifica le richieste di terminazione del processo (SIGTERM/SIGINT) per il graceful shutdown.
 *
 * @api
 */
interface SignalHandlerInterface
{
    /**
     * @param callable(int): void $onTerminate riceve il numero del segnale
     */
    public function onTerminate(callable $onTerminate): void;

    /**
     * Ripristina il comportamento di default dei segnali.
     */
    public function reset(): void;
}
