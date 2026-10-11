<?php

declare(strict_types=1);

namespace QueueBridge\Worker;

use QueueBridge\Message\Message;
use QueueBridge\Text\DecodedText;
use Throwable;

/**
 * Elabora i messaggi di un canale. Deve essere idempotente: con at-least-once lo stesso messaggio può
 * arrivare più volte.
 *
 * @api
 */
interface HandlerInterface
{
    /**
     * Complessità: Dipende dall'handler.
     *
     * @throws Throwable RetryableException per ritentare, UnrecoverableMessageException per la DLQ immediata
     */
    public function handle(Message $message, DecodedText $text): void;
}
