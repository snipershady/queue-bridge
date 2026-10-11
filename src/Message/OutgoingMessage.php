<?php

declare(strict_types=1);

namespace QueueBridge\Message;

use InvalidArgumentException;

/**
 * Messaggio da pubblicare, prima che la libreria gli assegni ID e data di creazione.
 *
 * @api
 */
final readonly class OutgoingMessage
{
    /**
     * Complessità: O(t + h), t = byte del testo, h = byte degli header (validazione UTF-8 e dei nomi).
     *
     * @param array<string, string> $headers
     * @param int                   $delayMs ritardo prima della consegna (>= 0)
     * @param ?int                  $ttlMs   scadenza dalla creazione (>= 1), null = nessuna
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public string $channel,
        public string $text,
        public array $headers = [],
        public int $delayMs = 0,
        public ?int $ttlMs = null,
    ) {
        MessageRules::assertChannel($channel);
        MessageRules::assertText($text);
        MessageRules::assertHeaders($headers);

        if ($delayMs < 0) {
            throw new InvalidArgumentException(\sprintf('Delay must be >= 0 ms, got %d.', $delayMs));
        }

        if (null !== $ttlMs && $ttlMs < 1) {
            throw new InvalidArgumentException(\sprintf('TTL must be >= 1 ms, got %d.', $ttlMs));
        }
    }
}
