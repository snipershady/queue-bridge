<?php

declare(strict_types=1);

namespace snipershady\Queue\Producer\Middleware;

use Override;
use snipershady\Queue\Correlation\CorrelationContext;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Producer\ProducerMiddlewareInterface;

/**
 * Imposta `correlation_id` (ereditato dal messaggio in elaborazione, altrimenti l'ID del messaggio stesso) e,
 * se il messaggio è pubblicato da un handler, `causation_id` con l'ID del messaggio che lo ha causato.
 * Non sovrascrive header già presenti.
 *
 * @api
 */
final readonly class CorrelationIdMiddleware implements ProducerMiddlewareInterface
{
    public function __construct(private CorrelationContext $context = new CorrelationContext())
    {
    }

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
