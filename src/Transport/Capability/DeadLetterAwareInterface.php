<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Transport\DeadLetter;

/**
 * Ispezione e riaccodamento della DLQ. I messaggi riaccodati ripartono con attempts = 0.
 *
 * @api
 */
interface DeadLetterAwareInterface
{
    /**
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     *
     * @return list<DeadLetter> i più vecchi per primi
     */
    public function deadLetters(int $limit = 100): array;

    /**
     * Riaccoda i $limit messaggi più vecchi; restituisce quanti ne ha riaccodati.
     *
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     */
    public function requeueDeadLetters(int $limit = 100): int;

    /**
     * @throws BackendException
     *
     * @return bool false se il messaggio non è in DLQ
     */
    public function requeueDeadLetter(MessageId $id): bool;

    /**
     * Svuota la DLQ; restituisce quanti messaggi ha rimosso.
     *
     * @throws BackendException
     */
    public function purgeDeadLetters(): int;
}
