<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Client;

use Override;
use Predis\Pipeline\Pipeline;

/**
 * Pipeline di Predis che restituisce gli errori dei singoli comandi invece di lanciare `ServerException`,
 * qualunque sia l'opzione `exceptions` del client. Con l'eccezione la pipeline smetterebbe di leggere le
 * risposte (e chiuderebbe la connessione) a metà, e non si saprebbe quali comandi il server ha eseguito.
 *
 * @internal
 */
final class PredisPipeline extends Pipeline
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    protected function throwServerExceptions(): bool
    {
        return false;
    }
}
