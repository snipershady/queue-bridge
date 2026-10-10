<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Exception\BatchSendException;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\Envelope;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;

/**
 * Porta verso il backend. Semantica at-least-once: un messaggio non viene mai perso, al più consegnato più volte.
 *
 * Le operazioni su una Delivery non più posseduta dal consumer (già confermata, o ripresa da un altro
 * consumer come orfana) non hanno effetto.
 *
 * @api
 */
interface TransportInterface
{
    /**
     * @throws InvalidArgumentException se $delayMs < 0
     * @throws BackendException
     */
    public function send(Message $message, int $delayMs = 0): void;

    /**
     * Invio in batch; ogni Envelope porta il proprio delay.
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
     * @throws InvalidArgumentException se $max < 1 o $timeoutMs < 0
     * @throws BackendException
     *
     * @return list<Delivery> al più $max elementi, in ordine di arrivo
     */
    public function receive(int $max = 1, int $timeoutMs = 1000): array;

    /**
     * @throws BackendException
     */
    public function ack(Delivery $delivery): void;

    /**
     * Rimette in coda con attempts + 1 dopo $delayMs, poi conferma l'originale. Il contatore delle
     * consegne riparte da zero. Garanzia: mai perso, al più duplicato.
     *
     * @throws InvalidArgumentException se $delayMs < 0
     * @throws BackendException
     */
    public function retry(Delivery $delivery, int $delayMs): void;

    /**
     * Sposta il messaggio in DLQ con il motivo e conferma l'originale. Garanzia: mai perso, al più duplicato.
     *
     * @throws BackendException
     */
    public function reject(Delivery $delivery, FailureReason $reason): void;

    /**
     * Restituisce subito alla coda un messaggio ricevuto ma non processato (shutdown, prefetch),
     * senza incrementare attempts.
     *
     * @throws BackendException
     */
    public function release(Delivery $delivery): void;

    /**
     * @throws ConnectionException se il backend non risponde
     */
    public function ping(): void;

    public function capabilities(): TransportCapabilities;
}
