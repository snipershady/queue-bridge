<?php

declare(strict_types=1);

namespace snipershady\Queue\Producer\Middleware;

use Override;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Producer\ProducerMiddlewareInterface;

/**
 * Propaga il trace context W3C (`traceparent`) nei messaggi pubblicati. Valori non conformi vengono ignorati;
 * un `traceparent` già presente non viene sovrascritto.
 *
 * @api
 */
final readonly class TraceContextMiddleware implements ProducerMiddlewareInterface
{
    public const string HEADER_TRACEPARENT = 'traceparent';

    private const string TRACEPARENT_PATTERN = '/^00-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}$/D';

    /**
     * Complessità: O(1).
     */
    public function __construct(private TraceContextProviderInterface $provider)
    {
    }

    /**
     * Complessità: O(t + h), t = byte del testo, h = byte degli header (validazione UTF-8 e dei nomi). Più il costo di
     * $next.
     */
    #[Override]
    public function process(Message $message, callable $next): void
    {
        $traceparent = $this->provider->currentTraceparent();

        if (\array_key_exists(self::HEADER_TRACEPARENT, $message->headers)
            || null === $traceparent
            || 1 !== \preg_match(self::TRACEPARENT_PATTERN, $traceparent)) {
            $next($message);

            return;
        }

        $next($message->withHeaders([...$message->headers, self::HEADER_TRACEPARENT => $traceparent]));
    }
}
