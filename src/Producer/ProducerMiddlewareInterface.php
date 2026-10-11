<?php

declare(strict_types=1);

namespace snipershady\Queue\Producer;

use snipershady\Queue\Message\Message;

/**
 * Elabora un messaggio prima dell'invio (es. aggiunta di header). Un middleware che non chiama $next
 * scarta il messaggio.
 *
 * @api
 */
interface ProducerMiddlewareInterface
{
    /**
     * Complessità: O(t + h) nei middleware forniti, più il costo di $next.
     *
     * @param callable(Message): void $next
     */
    public function process(Message $message, callable $next): void;
}
