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
     * @param callable(Message): void $next
     */
    public function process(Message $message, callable $next): void;
}
