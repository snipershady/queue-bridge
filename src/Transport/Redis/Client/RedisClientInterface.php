<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Client;

use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;

/**
 * Porta verso un client Redis (RESP2). Le risposte sono normalizzate: nil = null, status = stringa
 * (es. "OK"), interi = int, bulk string = string, array = list annidate.
 *
 * Per integrare un client proprietario basta implementare questa interfaccia.
 *
 * @api
 */
interface RedisClientInterface
{
    /**
     * Complessità: O(c + r), c = dimensione del comando, r = dimensione della risposta; un round trip.
     *
     * @param non-empty-list<string> $command nome del comando seguito dagli argomenti
     *
     * @throws CommandFailedException se il server risponde con un errore
     * @throws ConnectionException    se il server non è raggiungibile
     *
     * @return array<mixed>|int|string|null
     */
    public function execute(array $command): array|int|string|null;

    /**
     * Esegue i comandi con un solo round trip. Da usare solo con comandi che non rispondono mai nil
     * (es. XADD, ZADD): alcuni client non distinguono un nil da un errore dentro una pipeline.
     *
     * Complessità: O(c + r) sull'intera pipeline; un round trip.
     *
     * @param non-empty-list<non-empty-list<string>> $commands
     *
     * @throws ConnectionException se il server non è raggiungibile
     *
     * @return list<array<mixed>|int|string|CommandFailedException> una risposta (o l'errore) per comando, in ordine
     */
    public function pipeline(array $commands): array;
}
