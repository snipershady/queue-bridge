<?php

declare(strict_types=1);

namespace QueueBridge\Producer\Middleware;

use Override;
use QueueBridge\Correlation\CorrelationContext;
use QueueBridge\Message\Message;
use QueueBridge\Producer\ProducerMiddlewareInterface;

/**
 * Imposta `correlation_id` (ereditato dal messaggio in elaborazione, altrimenti l'ID del messaggio stesso) e,
 * se il messaggio è pubblicato da un handler, `causation_id` con l'ID del messaggio che lo ha causato.
 * Non sovrascrive header già presenti.
 *
 * @api
 */
final readonly class CorrelationIdMiddleware implements ProducerMiddlewareInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(private CorrelationContext $context = new CorrelationContext())
    {
    }

    /**
     * Complessità: O(t + h), t = byte del testo, h = byte degli header (validazione UTF-8 e dei nomi). Più il costo di
     * $next.
     */
    #[Override]
    public function process(Message $message, callable $next): void
    {
        $cause = $this->context->current();
        $headers = $message->headers;

        $headers[CorrelationContext::HEADER_CORRELATION_ID] ??= $cause?->headers[CorrelationContext::HEADER_CORRELATION_ID]
            ?? $cause?->id->value
            ?? $message->id->value;

        if ($cause instanceof Message) {
            $headers[CorrelationContext::HEADER_CAUSATION_ID] ??= $cause->id->value;
        }

        $next($message->withHeaders($headers));
    }
}
