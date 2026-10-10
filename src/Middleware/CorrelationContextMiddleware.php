<?php

declare(strict_types=1);

namespace snipershady\Queue\Middleware;

use Override;
use snipershady\Queue\Correlation\CorrelationContext;
use snipershady\Queue\Message\Message;

/**
 * Registra il messaggio in elaborazione nel CorrelationContext, per `CorrelationIdMiddleware` del producer.
 *
 * @api
 */
final readonly class CorrelationContextMiddleware implements MiddlewareInterface
{
    public function __construct(private CorrelationContext $context)
    {
    }

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
