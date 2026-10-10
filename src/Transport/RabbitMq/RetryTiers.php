<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq;

use InvalidArgumentException;

/**
 * Durate delle code di attesa. Per un ritardo si usa lo scaglione più grande non superiore al tempo residuo
 * (il più piccolo se il residuo è inferiore): il messaggio porta l'istante di scadenza e, se torna prima,
 * viene rimandato in attesa per il tempo che manca (CLAUDE.md §10).
 *
 * @api
 */
final readonly class RetryTiers
{
    /** @var non-empty-list<int> */
    public array $tiersMs;

    /**
     * @param list<int> $tiersMs durate in ms, strettamente crescenti e >= 1
     *
     * @throws InvalidArgumentException
     */
    public function __construct(array $tiersMs = [1000, 5000, 30_000, 120_000, 600_000, 3_600_000])
    {
        if ([] === $tiersMs) {
            throw new InvalidArgumentException('At least one retry tier is required.');
        }

        $previous = 0;
        foreach ($tiersMs as $tierMs) {
            if ($tierMs <= $previous) {
                throw new InvalidArgumentException(\sprintf('Retry tiers must be positive and strictly increasing, got %s.', implode(', ', $tiersMs)));
            }

            $previous = $tierMs;
        }

        $this->tiersMs = $tiersMs;
    }

    /**
     * Scaglione per un ritardo residuo di $remainingMs (> 0).
     */
    public function tierFor(int $remainingMs): int
    {
        $tier = $this->tiersMs[0];
        foreach ($this->tiersMs as $tierMs) {
            if ($tierMs <= $remainingMs) {
                $tier = $tierMs;
            }
        }

        return $tier;
    }
}
