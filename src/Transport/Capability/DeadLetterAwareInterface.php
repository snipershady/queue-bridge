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
 * Complessità dei metodi: t = byte di `text`, h = byte degli header, P = consegne pendenti, d = differiti,
 * T = scaglioni di attesa (RabbitMQ). "Round trip" = scambi di rete con il backend.
 *
 * @api
 */
interface DeadLetterAwareInterface
{
    /**
     * Complessità: O(limit) messaggi decodificati: un round trip su Redis, limit + 1 su RabbitMQ.
     *
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     *
     * @return list<DeadLetter> i più vecchi per primi
     */
    public function deadLetters(int $limit = 100): array;

    /**
     * Riaccoda i $limit messaggi più vecchi; restituisce quanti ne ha riaccodati.
     *
     * Complessità: O(limit): un round trip su Redis, limit + 1 su RabbitMQ.
     *
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     */
    public function requeueDeadLetters(int $limit = 100): int;

    /**
     * Complessità: O(L), L = messaggi in DLQ prima di quello cercato (ricerca lineare su ogni backend).
     *
     * @throws BackendException
     *
     * @return bool false se il messaggio non è in DLQ
     */
    public function requeueDeadLetter(MessageId $id): bool;

    /**
     * Svuota la DLQ; restituisce quanti messaggi ha rimosso.
     *
     * Complessità: O(1) round trip; lato backend O(L), L = messaggi in DLQ.
     *
     * @throws BackendException
     */
    public function purgeDeadLetters(): int;
}
