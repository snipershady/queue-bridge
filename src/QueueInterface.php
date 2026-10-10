<?php

declare(strict_types=1);

namespace snipershady\Queue;

use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Producer\ProducerInterface;
use snipershady\Queue\Transport\Capability\DeadLetterAwareInterface;
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

    public function capabilities(): TransportCapabilities;
}
