<?php

declare(strict_types=1);

namespace QueueBridge\Producer\Middleware;

/**
 * Ponte verso il sistema di tracing dell'applicazione (es. OpenTelemetry).
 *
 * @api
 */
interface TraceContextProviderInterface
{
    /**
     * Complessità: O(1).
     *
     * @return ?string header W3C `traceparent` dello span corrente; null se non c'è uno span attivo
     */
    public function currentTraceparent(): ?string;
}
