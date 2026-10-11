<?php

declare(strict_types=1);

namespace snipershady\Queue\Config;

use InvalidArgumentException;

/**
 * Configurazione della coda, identica per ogni backend: per cambiare backend si sostituisce solo il DTO
 * di strategia.
 *
 * @api
 */
final readonly class QueueConfig
{
    public const int MAX_NAME_LENGTH = 100;

    /** Nome e prefix compongono chiavi Redis e nomi RabbitMQ: niente separatori (`:` `.`) né hash tag (`{}`). */
    private const string NAME_PATTERN = '/^[A-Za-z0-9_-]+$/D';

    /**
     * @param string $name            nome della coda
     * @param string $prefix          prefisso di chiavi e risorse del backend
     * @param int    $maxTextBytes    dimensione massima di `text`, in byte
     * @param int    $maxHeadersBytes dimensione massima degli header (somma di nomi e valori), in byte
     * @param int    $maxAttempts     esecuzioni massime dell'handler per messaggio (1 = nessun retry)
     * @param int    $maxDeliveries   consegne massime senza conferma prima della DLQ (>= maxAttempts + 1)
     * @param int    $claimIdleMs     inattività dopo cui un messaggio in elaborazione è considerato orfano
     * @param int    $dlqMaxLength    retention della DLQ (numero di messaggi)
     *
     * @throws InvalidArgumentException se un valore è fuori dai limiti
     */
    public function __construct(
        public string $name = 'default',
        public string $prefix = 'rq',
        public int $maxTextBytes = 1_048_576,
        public int $maxHeadersBytes = 65_536,
        public int $maxAttempts = 3,
        public int $maxDeliveries = 10,
        public int $claimIdleMs = 300_000,
        public int $dlqMaxLength = 100_000,
    ) {
        $this->assertName('name', $name);
        $this->assertName('prefix', $prefix);
        $this->assertAtLeast('maxTextBytes', $maxTextBytes, 1);
        $this->assertAtLeast('maxHeadersBytes', $maxHeadersBytes, 0);
        $this->assertAtLeast('maxAttempts', $maxAttempts, 1);
        $this->assertAtLeast('maxDeliveries', $maxDeliveries, $maxAttempts + 1);
        $this->assertAtLeast('claimIdleMs', $claimIdleMs, 1000);
        $this->assertAtLeast('dlqMaxLength', $dlqMaxLength, 1);
    }

    private function assertName(string $field, string $value): void
    {
        if (\strlen($value) > self::MAX_NAME_LENGTH || 1 !== \preg_match(self::NAME_PATTERN, $value)) {
            throw new InvalidArgumentException(\sprintf('Invalid %s "%s": expected 1-%d characters matching %s.', $field, $value, self::MAX_NAME_LENGTH, self::NAME_PATTERN));
        }
    }

    private function assertAtLeast(string $field, int $value, int $minimum): void
    {
        if ($value < $minimum) {
            throw new InvalidArgumentException(\sprintf('%s must be >= %d, got %d.', $field, $minimum, $value));
        }
    }
}
