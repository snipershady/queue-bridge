<?php

declare(strict_types=1);

namespace snipershady\Queue\Retry;

/**
 * Decide il ritardo del prossimo tentativo. Il numero massimo di tentativi è `QueueConfig::$maxAttempts`:
 * la strategia può solo anticipare la rinuncia restituendo null.
 *
 * @api
 */
interface RetryStrategyInterface
{
    /**
     * @param int $attempt tentativo appena fallito (1 = la prima esecuzione)
     *
     * @return ?int ritardo in ms prima del prossimo tentativo; null = niente più retry
     */
    public function nextDelayMs(int $attempt): ?int;
}
