<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Support;

use Closure;
use Override;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpChannelInterface;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpMessage;

/**
 * Canale AMQP finto: registra le chiamate; `incoming` viene consegnato al consumer durante wait(),
 * `queues` alimenta get(), `failedConfirms` è restituito da confirm().
 */
final class FakeAmqpChannel implements AmqpChannelInterface
{
    /** @var list<string> */
    public array $calls = [];

    /** @var list<array{AmqpMessage, string, string}> messaggio, exchange, routing key */
    public array $published = [];

    /** @var list<int> */
    public array $acked = [];

    /** @var list<array{int, bool}> */
    public array $nacked = [];

    /** @var list<array{string, array<string, string|int>, bool}> */
    public array $declaredQueues = [];

    /** @var list<array{string, string}> */
    public array $declaredExchanges = [];

    /** @var list<array{string, string, string}> */
    public array $bindings = [];

    /** @var list<int> */
    public array $waits = [];

    /** @var list<list<AmqpMessage>> un gruppo di messaggi consegnati per ogni wait() */
    public array $incoming = [];

    /** @var array<string, list<AmqpMessage>> */
    public array $queues = [];

    /** @var array<string, array{int, int}|CommandFailedException> */
    public array $queueInfo = [];

    /** @var list<list<string>> */
    public array $failedConfirms = [];

    public ?ConnectionException $failNextPublish = null;

    public int $purged = 0;

    public bool $open = true;

    /** @var ?Closure(AmqpMessage): void */
    private ?Closure $consumer = null;

    #[Override]
    public function declareExchange(string $name, string $type, array $arguments = []): void
    {
        $this->calls[] = 'declareExchange';
        $this->declaredExchanges[] = [$name, $type];
    }

    #[Override]
    public function declareQueue(string $name, array $arguments = [], bool $passive = false): array
    {
        $this->calls[] = 'declareQueue';
        $this->declaredQueues[] = [$name, $arguments, $passive];
        $info = $this->queueInfo[$name] ?? [0, 0];
        if ($info instanceof CommandFailedException) {
            throw $info;
        }

        return $info;
    }

    #[Override]
    public function bindQueue(string $queue, string $exchange, string $routingKey): void
    {
        $this->calls[] = 'bindQueue';
        $this->bindings[] = [$queue, $exchange, $routingKey];
    }

    #[Override]
    public function purgeQueue(string $queue): int
    {
        $this->calls[] = 'purgeQueue:'.$queue;

        return $this->purged;
    }

    #[Override]
    public function prefetch(int $count): void
    {
        $this->calls[] = 'prefetch:'.$count;
    }

    #[Override]
    public function consume(string $queue, Closure $onMessage): void
    {
        $this->calls[] = 'consume:'.$queue;
        $this->consumer = $onMessage;
    }

    #[Override]
    public function wait(int $timeoutMs): bool
    {
        $this->waits[] = $timeoutMs;
        $batch = array_shift($this->incoming);
        $consumer = $this->consumer;
        if (null === $batch || !$consumer instanceof Closure) {
            return false;
        }

        foreach ($batch as $amqpMessage) {
            $consumer($amqpMessage);
        }

        return true;
    }

    #[Override]
    public function get(string $queue): ?AmqpMessage
    {
        $this->calls[] = 'get:'.$queue;

        return isset($this->queues[$queue]) ? array_shift($this->queues[$queue]) : null;
    }

    #[Override]
    public function publish(AmqpMessage $amqpMessage, string $exchange, string $routingKey): void
    {
        if ($this->failNextPublish instanceof ConnectionException) {
            $connectionException = $this->failNextPublish;
            $this->failNextPublish = null;

            throw $connectionException;
        }

        $this->published[] = [$amqpMessage, $exchange, $routingKey];
    }

    #[Override]
    public function confirm(int $timeoutMs): array
    {
        $this->calls[] = 'confirm:'.$timeoutMs;

        return array_shift($this->failedConfirms) ?? [];
    }

    #[Override]
    public function ack(int $deliveryTag): void
    {
        $this->acked[] = $deliveryTag;
    }

    #[Override]
    public function nack(int $deliveryTag, bool $requeue): void
    {
        $this->nacked[] = [$deliveryTag, $requeue];
    }

    #[Override]
    public function isOpen(): bool
    {
        return $this->open;
    }

    #[Override]
    public function close(): void
    {
        $this->calls[] = 'close';
        $this->open = false;
    }
}
