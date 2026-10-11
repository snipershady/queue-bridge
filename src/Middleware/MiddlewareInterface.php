<?php

declare(strict_types=1);

namespace snipershady\Queue\Middleware;

use snipershady\Queue\Message\Message;

/**
 * Avvolge l'esecuzione dell'handler nel worker. Un'eccezione che esce dal middleware viene trattata come
 * un fallimento dell'handler (retry o DLQ); un middleware che non chiama $next conferma il messaggio
 * senza eseguire l'handler.
 *
 * @api
 */
interface MiddlewareInterface
{
    /**
     * Complessità: O(1) nei middleware forniti, più il costo di $next.
     *
     * @param callable(Message): void $next
     */
    public function process(Message $message, callable $next): void;
}
