<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use snipershady\Queue\Exception\BackendException;

/**
 * Svuotamento della coda.
 *
 * @api
 */
interface PurgeAwareInterface
{
    /**
     * Rimuove i messaggi pronti e i differiti presenti al momento della chiamata. Non tocca i messaggi in
     * elaborazione (un worker li sta processando: confermati o tornati in coda, seguono il loro corso), la DLQ
     * (vedi DeadLetterAwareInterface::purgeDeadLetters()) né le risorse del backend: la coda resta utilizzabile.
     *
     * Complessità: O(n), n = messaggi rimossi (Redis: a pagine da 500).
     *
     * @throws BackendException
     *
     * @return int messaggi rimossi
     */
    public function purge(): int;
}
