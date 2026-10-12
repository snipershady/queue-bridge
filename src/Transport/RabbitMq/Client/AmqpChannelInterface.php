<?php

declare(strict_types=1);

namespace QueueBridge\Transport\RabbitMq\Client;

use Closure;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;

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
     * Complessità: O(1), un round trip.
     *
     * @param array<string, string|int> $arguments
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function declareExchange(string $name, string $type, array $arguments = []): void;

    /**
     * Complessità: O(1), un round trip.
     *
     * @param array<string, string|int> $arguments
     *
     * @throws CommandFailedException se `$passive` e la coda non esiste, o se gli argomenti differiscono
     * @throws ConnectionException
     *
     * @return array{int, int} messaggi pronti e consumer collegati
     */
    public function declareQueue(string $name, array $arguments = [], bool $passive = false): array;

    /**
     * Complessità: O(1), un round trip.
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function bindQueue(string $queue, string $exchange, string $routingKey): void;

    /**
     * Complessità: O(1) round trip; lato broker O(q), q = messaggi rimossi.
     *
     * @throws CommandFailedException|ConnectionException
     *
     * @return int messaggi rimossi
     */
    public function purgeQueue(string $queue): int;

    /**
     * Complessità: O(1), un round trip.
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function prefetch(int $count): void;

    /**
     * Avvia un consumer: i messaggi arrivano a $onMessage durante wait().
     *
     * Complessità: O(1), un round trip.
     *
     * @param Closure(AmqpMessage): void $onMessage
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function consume(string $queue, Closure $onMessage): void;

    /**
     * Elabora i frame in arrivo per al più $timeoutMs (0 = senza attendere).
     *
     * Complessità: O(e), e = frame già arrivati; attende al più $timeoutMs.
     *
     * @throws ConnectionException
     *
     * @return bool false se è scaduto il timeout senza ricevere nulla
     */
    public function wait(int $timeoutMs): bool;

    /**
     * Preleva un messaggio senza consumer (basic.get), da confermare o restituire.
     *
     * Complessità: O(b), b = byte del messaggio; un round trip.
     *
     * @throws CommandFailedException|ConnectionException
     */
    public function get(string $queue): ?AmqpMessage;

    /**
     * Complessità: O(b), b = byte del messaggio; nessuna attesa.
     *
     * @throws ConnectionException
     */
    public function publish(AmqpMessage $amqpMessage, string $exchange, string $routingKey): void;

    /**
     * Attende le conferme del broker per i messaggi pubblicati dall'ultima chiamata. Un'implementazione deve
     * usare davvero le publisher confirms (e i `basic.return`): retry, reject e riaccodamento confermano
     * l'originale solo se questo metodo non segnala la copia come fallita, e una lista vuota senza conferme
     * reali può far perdere messaggi.
     *
     * Complessità: O(c), c = publish in attesa di conferma; attende al più $timeoutMs.
     *
     * @throws ConnectionException
     *
     * @return list<string> messageId dei messaggi rifiutati (nack) o non instradabili (return)
     */
    public function confirm(int $timeoutMs): array;

    /**
     * Complessità: O(1), nessuna attesa.
     *
     * @throws ConnectionException
     */
    public function ack(int $deliveryTag): void;

    /**
     * Complessità: O(1), nessuna attesa.
     *
     * @throws ConnectionException
     */
    public function nack(int $deliveryTag, bool $requeue): void;

    /**
     * Complessità: O(1).
     */
    public function isOpen(): bool;

    /**
     * Chiude il canale: i messaggi consegnati e non confermati tornano in coda. Non lancia eccezioni.
     *
     * Complessità: O(1), un round trip.
     */
    public function close(): void;
}
