<?php

declare(strict_types=1);

namespace snipershady\Queue\Idempotency;

/**
 * Esito di `IdempotencyStoreInterface::acquire()`.
 *
 * @api
 */
enum AcquireStatus: string
{
    /**
     * Lock acquisito: l'handler va eseguito.
     */
    case Acquired = 'acquired';

    /**
     * Un altro worker sta elaborando il messaggio: va ritentato, non confermato.
     */
    case InProgress = 'in_progress';

    /**
     * Il messaggio è già stato elaborato: va confermato senza eseguire l'handler.
     */
    case Completed = 'completed';
}
