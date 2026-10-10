<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq\Client;

use Closure;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;

/**
 * Porta verso un canale AMQP 0-9-1. Exchange e code sono sempre durevoli; i messaggi pubblicati sono
 * persistenti e `mandatory`. Dopo un'eccezione il canale non è più utilizzabile (in AMQP un errore chiude
 * il canale): va riaperto tramite AmqpConnectorInterface.
 *
 * @api
 */
interface AmqpChannelInterface
{
    /**
     * @param array<string, string|int> $arguments
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function declareExchange(string $name, string $type, array $arguments = []): void;

    /**
     * @param array<string, string|int> $arguments
     *
     * @throws CommandFailedException se `$passive` e la coda non esiste, o se gli argomenti differiscono
     * @throws ConnectionException
     *
     * @return array{int, int} messaggi pronti e consumer collegati
     */
    public function declareQueue(string $name, array $arguments = [], bool $passive = false): array;

    /**
     * @throws CommandFailedException|ConnectionException
     */
    public function bindQueue(string $queue, string $exchange, string $routingKey): void;

    /**
     * @throws CommandFailedException|ConnectionException
     *
     * @return int messaggi rimossi
     */
    public function purgeQueue(string $queue): int;

    /**
     * @throws CommandFailedException|ConnectionException
     */
    public function prefetch(int $count): void;

    /**
     * Avvia un consumer: i messaggi arrivano a $onMessage durante wait().
     *
     * @param Closure(AmqpMessage): void $onMessage
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function consume(string $queue, Closure $onMessage): void;

    /**
     * Elabora i frame in arrivo per al più $timeoutMs (0 = senza attendere).
     *
     * @throws ConnectionException
     *
     * @return bool false se è scaduto il timeout senza ricevere nulla
     */
    public function wait(int $timeoutMs): bool;

    /**
     * Preleva un messaggio senza consumer (basic.get), da confermare o restituire.
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function get(string $queue): ?AmqpMessage;

    /**
     * @throws ConnectionException
     */
    public function publish(AmqpMessage $amqpMessage, string $exchange, string $routingKey): void;

    /**
     * Attende le conferme del broker per i messaggi pubblicati dall'ultima chiamata.
     *
     * @throws ConnectionException
     *
     * @return list<string> messageId dei messaggi rifiutati (nack) o non instradabili (return)
     */
    public function confirm(int $timeoutMs): array;

    /**
     * @throws ConnectionException
     */
    public function ack(int $deliveryTag): void;

    /**
     * @throws ConnectionException
     */
    public function nack(int $deliveryTag, bool $requeue): void;

    public function isOpen(): bool;

    /**
     * Chiude il canale: i messaggi consegnati e non confermati tornano in coda. Non lancia eccezioni.
     */
    public function close(): void;
}
