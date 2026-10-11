<?php

declare(strict_types=1);

namespace QueueBridge\Middleware;

use Override;
use QueueBridge\Correlation\CorrelationContext;
use QueueBridge\Message\Message;

/**
 * Registra il messaggio in elaborazione nel CorrelationContext, per `CorrelationIdMiddleware` del producer.
 *
 * @api
 */
final readonly class CorrelationContextMiddleware implements MiddlewareInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(private CorrelationContext $context)
    {
    }

    /**
     * Complessità: O(1) più il costo di $next.
     */
    #[Override]
    public function process(Message $message, callable $next): void
    {
        $this->context->enter($message);

        try {
            $next($message);
        } finally {
            $this->context->leave();
        }
    }
}
