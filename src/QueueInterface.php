<?php

declare(strict_types=1);

namespace QueueBridge;

use InvalidArgumentException;
use QueueBridge\Exception\BackendException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Producer\ProducerInterface;
use QueueBridge\Transport\Capability\DeadLetterAwareInterface;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;
use QueueBridge\Worker\WorkerConfig;
use QueueBridge\Worker\WorkerInterface;

/**
 * Una coda su un backend, ottenuta da QueueFactoryInterface::create().
 *
 * @api
 */
interface QueueInterface
{
    /**
     * Crea le risorse del backend, se ne ha bisogno. Idempotente.
     *
     * Complessità: Quella di `setup()` del transport: O(1) su Redis e InMemory, O(T) dichiarazioni su RabbitMQ (T =
     * scaglioni).
     *
     * @throws BackendException
     */
    public function setup(): void;

    /**
     * Complessità: O(1).
     */
    public function producer(): ProducerInterface;

    /**
     * Un worker nuovo a ogni chiamata (handler e stato non sono condivisi).
     *
     * Complessità: O(m), m = middleware del worker.
     */
    public function worker(WorkerConfig $config = new WorkerConfig()): WorkerInterface;

    /**
     * Complessità: O(1).
     */
    public function transport(): TransportInterface;

    /**
     * Complessità: O(1), un round trip.
     *
     * @throws ConnectionException
     */
    public function ping(): void;

    /**
     * Complessità: Quella di `stats()` del transport.
     *
     * @throws BackendException
     *
     * @return ?QueueStats null se il backend non fornisce statistiche
     */
    public function stats(): ?QueueStats;

    /**
     * Complessità: O(1).
     *
     * @return ?DeadLetterAwareInterface null se il backend non ha una DLQ ispezionabile
     */
    public function deadLetters(): ?DeadLetterAwareInterface;

    /**
     * Svuota la coda: messaggi pronti e differiti. In elaborazione e DLQ restano (vedi PurgeAwareInterface).
     *
     * Complessità: O(n) nei messaggi rimossi (vedi `purge()` del transport).
     *
     * @throws BackendException
     *
     * @return ?int messaggi rimossi; null se il backend non permette di svuotare la coda
     */
    public function purge(): ?int;

    /**
     * Elenca i messaggi in coda senza consumarli (vedi BrowseAwareInterface).
     *
     * Complessità: O(limit) messaggi decodificati (vedi `browse()` del transport).
     *
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     *
     * @return ?list<QueuedMessage> null se il backend non permette di elencare i messaggi
     */
    public function browse(int $limit = 100): ?array;

    /**
     * Complessità: O(1).
     */
    public function capabilities(): TransportCapabilities;
}
