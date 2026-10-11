<?php

declare(strict_types=1);

namespace QueueBridge\Transport;

use InvalidArgumentException;
use QueueBridge\Exception\BackendException;
use QueueBridge\Exception\BatchSendException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;

/**
 * Porta verso il backend. Semantica at-least-once: un messaggio non viene mai perso, al più consegnato più volte.
 *
 * Le operazioni su una Delivery non più posseduta dal consumer (già confermata, o ripresa da un altro
 * consumer come orfana) non hanno effetto.
 *
 * Complessità dei metodi: t = byte di `text`, h = byte degli header, P = consegne pendenti, d = differiti,
 * T = scaglioni di attesa (RabbitMQ). "Round trip" = scambi di rete con il backend.
 *
 * @api
 */
interface TransportInterface
{
    /**
     * Complessità: O(t + h) per la codifica, un round trip (su RabbitMQ con attesa della conferma), t = byte del testo,
     * h = byte degli header.
     *
     * @throws InvalidArgumentException se $delayMs < 0
     * @throws BackendException
     */
    public function send(Message $message, int $delayMs = 0): void;

    /**
     * Invio in batch; ogni Envelope porta il proprio delay.
     *
     * Complessità: O(n·(t + h)), n = messaggi; un round trip su Redis (pipeline), una attesa di conferma su RabbitMQ.
     *
     * @param iterable<Envelope> $envelopes
     *
     * @throws BatchSendException con gli ID inviati e quelli falliti
     * @throws BackendException
     */
    public function sendBatch(iterable $envelopes): void;

    /**
     * Attende fino a $timeoutMs; ritorna lista vuota se non c'è nulla.
     *
     * Complessità: O(k·(t + h)), k = consegne restituite (al più $max); un numero costante di round trip.
     *
     * @throws InvalidArgumentException se $max < 1 o $timeoutMs < 0
     * @throws BackendException
     *
     * @return list<Delivery> al più $max elementi, in ordine di arrivo
     */
    public function receive(int $max = 1, int $timeoutMs = 1000): array;

    /**
     * Complessità: O(1); un round trip su Redis, nessuna attesa su RabbitMQ.
     *
     * @throws BackendException
     */
    public function ack(Delivery $delivery): void;

    /**
     * Rimette in coda con attempts + 1 dopo $delayMs, poi conferma l'originale. Il contatore delle
     * consegne riparte da zero. Garanzia: mai perso, al più duplicato.
     *
     * Complessità: O(t + h); un round trip (script atomico su Redis, conferma del broker su RabbitMQ).
     *
     * @throws InvalidArgumentException se $delayMs < 0
     * @throws BackendException
     */
    public function retry(Delivery $delivery, int $delayMs): void;

    /**
     * Sposta il messaggio in DLQ con il motivo e conferma l'originale. Garanzia: mai perso, al più duplicato.
     *
     * Complessità: O(t + h); un round trip (script atomico su Redis, conferma del broker su RabbitMQ).
     *
     * @throws BackendException
     */
    public function reject(Delivery $delivery, FailureReason $reason): void;

    /**
     * Restituisce subito alla coda un messaggio ricevuto ma non processato (shutdown, prefetch),
     * senza incrementare attempts.
     *
     * Complessità: O(t + h) su Redis (un round trip), O(1) senza attesa su RabbitMQ.
     *
     * @throws BackendException
     */
    public function release(Delivery $delivery): void;

    /**
     * Complessità: O(1), un round trip.
     *
     * @throws ConnectionException se il backend non risponde
     */
    public function ping(): void;

    /**
     * Complessità: O(1).
     */
    public function capabilities(): TransportCapabilities;
}
