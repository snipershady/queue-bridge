<?php

declare(strict_types=1);

namespace QueueBridge\Transport\RabbitMq;

use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\ConnectionException;
use QueueBridge\Transport\RabbitMq\Client\AmqpChannelInterface;

/**
 * Exchange e code di una coda logica (prefix `rq`, coda `messages`):
 *
 * - `rq.messages` exchange topic, con binding `#` verso la coda omonima;
 * - `rq.messages` quorum queue, `x-delivery-limit = maxDeliveries` e dead-letter at-least-once verso la DLQ
 *   (rete di sicurezza: il worker sposta in DLQ prima, con il motivo);
 * - `rq.messages.dlq` classic queue: l'ispezione con get + requeue non deve far crescere un delivery-limit;
 * - `rq.messages.retry.<ms>` quorum queue a TTL fisso, che fanno dead-letter at-least-once verso la coda principale.
 *
 * @internal
 */
final readonly class Topology
{
    public string $exchange;

    public string $queue;

    public string $deadLetterQueue;

    /**
     * Complessità: O(1).
     */
    public function __construct(
        private QueueConfig $config,
        private RetryTiers $retryTiers,
    ) {
        $this->exchange = $config->prefix.'.'.$config->name;
        $this->queue = $this->exchange;
        $this->deadLetterQueue = $this->exchange.'.dlq';
    }

    /**
     * Complessità: O(1).
     */
    public function retryQueue(int $tierMs): string
    {
        return $this->queue.'.retry.'.$tierMs;
    }

    /**
     * Complessità: O(T), T = scaglioni.
     *
     * @return list<string>
     */
    public function retryQueues(): array
    {
        return array_map($this->retryQueue(...), $this->retryTiers->tiersMs);
    }

    /**
     * Idempotente: dichiarare di nuovo con gli stessi argomenti non ha effetto.
     *
     * Complessità: O(T), T = scaglioni: T + 4 round trip.
     *
     * @throws CommandFailedException se una coda esiste già con argomenti diversi
     * @throws ConnectionException
     */
    public function declare(AmqpChannelInterface $amqpChannel): void
    {
        $amqpChannel->declareExchange($this->exchange, 'topic');
        $amqpChannel->declareQueue($this->queue, [
            'x-queue-type' => 'quorum',
            'x-delivery-limit' => $this->config->maxDeliveries,
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $this->deadLetterQueue,
            'x-dead-letter-strategy' => 'at-least-once',
            'x-overflow' => 'reject-publish',
        ]);
        $amqpChannel->bindQueue($this->queue, $this->exchange, '#');
        $amqpChannel->declareQueue($this->deadLetterQueue, ['x-queue-type' => 'classic']);

        foreach ($this->retryTiers->tiersMs as $tierMs) {
            $amqpChannel->declareQueue($this->retryQueue($tierMs), [
                'x-queue-type' => 'quorum',
                'x-message-ttl' => $tierMs,
                'x-dead-letter-exchange' => '',
                'x-dead-letter-routing-key' => $this->queue,
                'x-dead-letter-strategy' => 'at-least-once',
                'x-overflow' => 'reject-publish',
            ]);
        }
    }
}
