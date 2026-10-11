<?php

declare(strict_types=1);

namespace snipershady\Queue;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Producer\ProducerInterface;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
use snipershady\Queue\Transport\QueuedMessage;
use snipershady\Queue\Transport\QueueStats;
use snipershady\Queue\Transport\TransportCapabilities;
use snipershady\Queue\Transport\TransportInterface;
use snipershady\Queue\Worker\WorkerConfig;
use snipershady\Queue\Worker\WorkerInterface;

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
     * @throws BackendException
     */
    public function setup(): void;

    public function producer(): ProducerInterface;

    /**
     * Un worker nuovo a ogni chiamata (handler e stato non sono condivisi).
     */
    public function worker(WorkerConfig $config = new WorkerConfig()): WorkerInterface;

    public function transport(): TransportInterface;

    /**
     * @throws ConnectionException
     */
    public function ping(): void;

    /**
     * @throws BackendException
     *
     * @return ?QueueStats null se il backend non fornisce statistiche
     */
    public function stats(): ?QueueStats;

    /**
     * @return ?DeadLetterAwareInterface null se il backend non ha una DLQ ispezionabile
     */
    public function deadLetters(): ?DeadLetterAwareInterface;

    /**
     * Svuota la coda: messaggi pronti e differiti. In elaborazione e DLQ restano (vedi PurgeAwareInterface).
     *
     * @throws BackendException
     *
     * @return ?int messaggi rimossi; null se il backend non permette di svuotare la coda
     */
    public function purge(): ?int;

    /**
     * Elenca i messaggi in coda senza consumarli (vedi BrowseAwareInterface).
     *
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     *
     * @return ?list<QueuedMessage> null se il backend non permette di elencare i messaggi
     */
    public function browse(int $limit = 100): ?array;

    public function capabilities(): TransportCapabilities;
}
