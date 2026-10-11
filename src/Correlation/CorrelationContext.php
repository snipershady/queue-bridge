<?php

declare(strict_types=1);

namespace QueueBridge\Correlation;

use QueueBridge\Message\Message;

/**
 * Messaggio in elaborazione nel worker, condiviso con il producer: così i messaggi pubblicati da un handler
 * ereditano `correlation_id` e ricevono `causation_id`. Un'istanza per processo, iniettata in entrambi.
 *
 * @api
 */
final class CorrelationContext
{
    public const string HEADER_CORRELATION_ID = 'correlation_id';

    public const string HEADER_CAUSATION_ID = 'causation_id';

    /** @var list<Message> */
    private array $stack = [];

    /**
     * Complessità: O(1) ammortizzato.
     */
    public function enter(Message $message): void
    {
        $this->stack[] = $message;
    }

    /**
     * Complessità: O(1).
     */
    public function leave(): void
    {
        array_pop($this->stack);
    }

    /**
     * Complessità: O(1).
     */
    public function current(): ?Message
    {
        return $this->stack[\count($this->stack) - 1] ?? null;
    }
}
