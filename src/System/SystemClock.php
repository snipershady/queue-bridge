<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

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

    public function __construct()
    {
        $this->dateTimeZone = new DateTimeZone('UTC');
    }

    #[Override]
    public function now(): DateTimeImmutable
    {
        return new DateTimeImmutable('now', $this->dateTimeZone);
    }
}
