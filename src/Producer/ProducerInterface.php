<?php

declare(strict_types=1);

namespace snipershady\Queue\Producer;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Exception\BatchSendException;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\OutgoingMessage;

/**
 * @api
 */
interface ProducerInterface
{
    /**
     * Complessità: O(m·(t + h)) più un invio al transport, t = byte del testo, h = byte degli header, m = middleware
     * del producer.
     *
     * @param array<string, string> $headers
     * @param int                   $delayMs ritardo prima della consegna
     * @param ?int                  $ttlMs   scadenza dalla creazione: oltre, il messaggio non viene processato
     *
     * @throws InvalidArgumentException se il messaggio non è valido o supera i limiti di QueueConfig
     * @throws BackendException
     */
    public function publish(string $channel, string $text, array $headers = [], int $delayMs = 0, ?int $ttlMs = null): MessageId;

    /**
     * Complessità: O(n·m·(t + h)) più un invio in batch al transport, n = messaggi, m = middleware del producer.
     *
     * @param iterable<OutgoingMessage> $messages
     *
     * @throws InvalidArgumentException se un messaggio supera i limiti di QueueConfig (nessuno viene inviato)
     * @throws BatchSendException       con gli ID inviati e quelli falliti
     * @throws BackendException
     *
     * @return list<MessageId> ID dei messaggi inviati, nello stesso ordine
     */
    public function publishBatch(iterable $messages): array;
}
