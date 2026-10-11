<?php

declare(strict_types=1);

namespace snipershady\Queue\System;

use Override;
use Psr\Clock\ClockInterface;
use snipershady\Queue\Message\MessageId;

/**
 * Generatore UUIDv7 (RFC 9562) monotono.
 *
 * Layout: 48 bit di timestamp Unix in ms | versione 7 | 12 bit di contatore (rand_a) | variante | 62 bit casuali.
 * Il contatore parte da un valore casuale a 11 bit a ogni nuovo millisecondo e si incrementa nello stesso ms
 * (RFC 9562 §6.2, metodo 1). In caso di overflow o di orologio che torna indietro il timestamp avanza
 * logicamente, così gli ID restano strettamente crescenti.
 *
 * @api
 */
final class UuidV7Generator implements IdGeneratorInterface
{
    private const int COUNTER_MAX = 0xFFF;

    private const int COUNTER_SEED_MASK = 0x7FF;

    private int $lastMs = -1;

    private int $counter = 0;

    public function __construct(
        private readonly ClockInterface $clock,
        private readonly RandomInterface $random = new NativeRandom(),
    ) {
    }

    #[Override]
    public function generate(): MessageId
    {
        $nowMs = (int) $this->clock->now()->format('Uv');

        if ($nowMs > $this->lastMs) {
            $this->lastMs = $nowMs;
            $seed = $this->random->bytes(2);
            $this->counter = ((\ord($seed[0]) << 8) | \ord($seed[1])) & self::COUNTER_SEED_MASK;
        } elseif ($this->counter < self::COUNTER_MAX) {
            ++$this->counter;
        } else {
            ++$this->lastMs;
            $this->counter = 0;
        }

        $tail = $this->random->bytes(8);
        $head = pack('J', ($this->lastMs << 16) | 0x7000 | $this->counter);
        $variant = \chr(0x80 | (\ord($tail[0]) & 0x3F));

        return MessageId::fromBytes($head.$variant.\substr($tail, 1));
    }
}
