<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use snipershady\Queue\Message\MessageId;
use Throwable;

/**
 * Invio in batch fallito in parte: indica quali messaggi sono stati accettati dal backend e quali no.
 *
 * @api
 */
final class BatchSendException extends BackendException
{
    /**
     * @param list<MessageId> $sentIds
     * @param list<MessageId> $failedIds
     */
    public function __construct(
        public readonly array $sentIds,
        public readonly array $failedIds,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            \sprintf('Batch send failed: %d sent, %d failed.', \count($sentIds), \count($failedIds)),
            0,
            $previous,
        );
    }
}
