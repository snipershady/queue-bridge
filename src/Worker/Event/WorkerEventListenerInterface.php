<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker\Event;

/**
 * Un listener che lancia un'eccezione non ferma il worker: l'errore viene loggato.
 *
 * @api
 */
interface WorkerEventListenerInterface
{
    public function onEvent(WorkerEventInterface $event): void;
}
