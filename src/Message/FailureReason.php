<?php

declare(strict_types=1);

namespace snipershady\Queue\Message;

use DateTimeImmutable;
use DateTimeZone;
use Throwable;

/**
 * Motivo del passaggio in DLQ.
 *
 * @api
 */
final readonly class FailureReason
{
    /** Limite in byte del messaggio d'errore salvato, per non gonfiare la DLQ. */
    public const int MAX_ERROR_MESSAGE_BYTES = 4096;

    public string $errorMessage;

    public DateTimeImmutable $failedAt;

    /**
     * @param ?string $errorClass nome della classe dell'eccezione; null se il motivo non deriva da un'eccezione
     */
    public function __construct(
        public FailureKind $kind,
        public MessageId $originalId,
        DateTimeImmutable $failedAt,
        string $errorMessage = '',
        public ?string $errorClass = null,
    ) {
        $this->errorMessage = MessageRules::truncateUtf8(
            MessageRules::isValidUtf8($errorMessage) ? $errorMessage : (string) preg_replace('/[\x80-\xFF]/', '?', $errorMessage),
            self::MAX_ERROR_MESSAGE_BYTES,
        );
        $this->failedAt = $failedAt->setTimezone(new DateTimeZone('UTC'));
    }

    public static function fromThrowable(
        FailureKind $kind,
        MessageId $originalId,
        DateTimeImmutable $failedAt,
        Throwable $throwable,
    ): self {
        return new self($kind, $originalId, $failedAt, $throwable->getMessage(), $throwable::class);
    }
}
