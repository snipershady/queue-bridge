<?php

declare(strict_types=1);

namespace QueueBridge\System;

use DateTimeImmutable;
use DateTimeZone;
use Override;
use Psr\Clock\ClockInterface;

/**
 * Orologio di sistema in UTC (PSR-20).
 *
 * @api
 */
final readonly class SystemClock implements ClockInterface
{
    private DateTimeZone $dateTimeZone;

    /**
     * Complessità: O(1).
     */
    public function __construct()
    {
        $this->dateTimeZone = new DateTimeZone('UTC');
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function now(): DateTimeImmutable
    {
        return new DateTimeImmutable('now', $this->dateTimeZone);
    }
}
