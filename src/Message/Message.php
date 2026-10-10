<?php

declare(strict_types=1);

namespace snipershady\Queue\Message;

use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;

/**
 * Messaggio logico, indipendente dal backend. Immutabile.
 *
 * @api
 */
final readonly class Message
{
    public DateTimeImmutable $createdAt;

    public ?DateTimeImmutable $expiresAt;

    /**
     * @param string                $text     contenuto arbitrario, UTF-8 valido
     * @param int                   $attempts tentativi falliti già fatti
     * @param array<string, string> $headers
     *
     * @throws InvalidArgumentException se un campo viola le invarianti
     */
    public function __construct(
        public MessageId $id,
        public string $channel,
        public string $text,
        DateTimeImmutable $createdAt,
        public int $attempts = 0,
        ?DateTimeImmutable $expiresAt = null,
        public array $headers = [],
    ) {
        MessageRules::assertChannel($channel);
        MessageRules::assertText($text);
        MessageRules::assertHeaders($headers);

        if ($attempts < 0) {
            throw new InvalidArgumentException(\sprintf('Attempts must be >= 0, got %d.', $attempts));
        }

        if ($expiresAt instanceof DateTimeImmutable && $expiresAt <= $createdAt) {
            throw new InvalidArgumentException('Expiration must be later than creation.');
        }

        $dateTimeZone = new DateTimeZone('UTC');
        $this->createdAt = $createdAt->setTimezone($dateTimeZone);
        $this->expiresAt = $expiresAt?->setTimezone($dateTimeZone);
    }

    /**
     * Copia con un tentativo fallito in più: stesso ID, stessi dati.
     */
    public function withIncrementedAttempts(): self
    {
        return $this->withAttempts($this->attempts + 1);
    }

    /**
     * Copia con il numero di tentativi indicato (es. 0 quando si riaccoda dalla DLQ).
     *
     * @throws InvalidArgumentException se $attempts < 0
     */
    public function withAttempts(int $attempts): self
    {
        return new self(
            $this->id,
            $this->channel,
            $this->text,
            $this->createdAt,
            $attempts,
            $this->expiresAt,
            $this->headers,
        );
    }

    /**
     * Copia con gli header indicati al posto di quelli attuali.
     *
     * @param array<string, string> $headers
     *
     * @throws InvalidArgumentException se un header non è valido
     */
    public function withHeaders(array $headers): self
    {
        return new self(
            $this->id,
            $this->channel,
            $this->text,
            $this->createdAt,
            $this->attempts,
            $this->expiresAt,
            $headers,
        );
    }

    public function isExpired(DateTimeImmutable $now): bool
    {
        return $this->expiresAt instanceof DateTimeImmutable && $now >= $this->expiresAt;
    }
}
