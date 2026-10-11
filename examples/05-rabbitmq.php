<?php

declare(strict_types=1);

/*
 * RabbitMQ integration: custom retry tiers and prefetch, publisher confirms, retry through a wait queue,
 * dead letter queue.
 *
 *   php examples/05-rabbitmq.php
 *
 * AMQP_HOST / AMQP_PORT / AMQP_USER / AMQP_PASSWORD select the broker (default: docker-compose.yml, 127.0.0.1:35672).
 */

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\RetryableException;
use QueueBridge\Exception\UnrecoverableMessageException;
use QueueBridge\Message\Message;
use QueueBridge\QueueBridge;
use QueueBridge\Retry\FixedDelay;
use QueueBridge\Text\DecodedText;
use QueueBridge\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use QueueBridge\Transport\RabbitMq\RetryTiers;
use QueueBridge\Transport\RabbitMq\StrategyDtoRabbitMq;
use QueueBridge\Worker\WorkerConfig;
use QueueBridgeExamples\Backend;

require __DIR__.'/../vendor/autoload.php';
require_once __DIR__.'/Support/Backend.php';

// The connector reopens the channel after every AMQP error and, with a factory, the whole connection.
$connector = new PhpAmqpLibConnector(
    static fn (): AbstractConnection => new AMQPStreamConnection(
        Backend::env('AMQP_HOST', '127.0.0.1'),
        (int) Backend::env('AMQP_PORT', '35672'),
        Backend::env('AMQP_USER', 'guest'),
        Backend::env('AMQP_PASSWORD', 'guest'),
        heartbeat: 0, // see the README: a synchronous worker cannot answer heartbeats during a long handler
    ),
    publisherConfirms: true,
);

// Delays and retries use wait queues with fixed TTLs: one per tier (here 1 s, 10 s, 1 min).
$strategy = new StrategyDtoRabbitMq($connector, retryTiers: new RetryTiers([1_000, 10_000, 60_000]), prefetch: 20);

// maxDeliveries becomes the x-delivery-limit of the quorum queue: changing it later requires recreating the queue.
$queue = QueueBridge::factory()->create($strategy, new QueueConfig(name: 'example-rabbitmq', prefix: 'examples', maxAttempts: 3));
$queue->setup(); // exchange, quorum queue, wait queues and DLQ

$producer = $queue->producer();
$producer->publish('payments', '{"payment":"P-1","amount":"49.00"}');
$producer->publish('payments', '{"payment":"P-2","amount":"-1"}');
$producer->publish('payments', '{"payment":"P-3","amount":"15.00","gateway":"slow"}');

$attempts = [];
$worker = $queue->worker(new WorkerConfig(
    receiveTimeoutMs: 3_000, // longer than the first tier, so the retried message comes back before the worker stops
    stopWhenEmpty: true,
    retryStrategy: new FixedDelay(1_000),
))->on('payments', static function (Message $message, DecodedText $text) use (&$attempts): void {
    $payment = is_array($text->value) ? $text->value : throw new UnrecoverableMessageException('Not a JSON object.');
    $id = is_string($payment['payment'] ?? null) ? $payment['payment'] : '?';
    $attempts[$id] = ($attempts[$id] ?? 0) + 1;

    if (!is_numeric($payment['amount'] ?? null) || $payment['amount'] <= 0) {
        throw new UnrecoverableMessageException(sprintf('Invalid amount for %s.', $id));
    }

    if ('slow' === ($payment['gateway'] ?? null) && 0 === $message->attempts) {
        // Transient failure: the message goes to the 1 s wait queue and comes back with attempts = 1.
        throw new RetryableException('Gateway timeout.');
    }

    printf("  %s charged %s (attempt %d, message %s)\n", $id, $payment['amount'], $message->attempts + 1, $message->id->value);
});

printf("Worker stopped: %s\n", $worker->run()->value);
printf("Handler calls per payment: %s\n", json_encode($attempts, JSON_THROW_ON_ERROR));

$deadLetters = $queue->deadLetters();
foreach ($deadLetters?->deadLetters() ?? [] as $deadLetter) {
    printf("Dead letter %s: %s\n", $deadLetter->message->text, $deadLetter->reason->errorMessage);
}

printf("Purged %d dead letters.\n", $deadLetters?->purgeDeadLetters() ?? 0);
