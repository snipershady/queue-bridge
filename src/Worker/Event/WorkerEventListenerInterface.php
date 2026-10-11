<?php

declare(strict_types=1);

namespace QueueBridge\Worker\Event;

/**
 * Un listener che lancia un'eccezione non ferma il worker: l'errore viene loggato.
 *
 * @api
 */
interface WorkerEventListenerInterface
{
    /**
     * Complessità: Dipende dal listener; viene chiamato più volte per ogni messaggio, quindi dovrebbe essere O(1).
     */
    public function onEvent(WorkerEventInterface $event): void;
}
