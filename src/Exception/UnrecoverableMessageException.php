<?php

declare(strict_types=1);

namespace QueueBridge\Exception;

use Throwable;

/**
 * Lanciata da un handler quando ritentare è inutile (dati invalidi, entità inesistente): il messaggio va
 * subito in DLQ.
 *
 * @api
 */
final class UnrecoverableMessageException extends QueueException
{
    /**
     * Complessità: O(1).
     */
    public function __construct(string $message = '', ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
