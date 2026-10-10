<?php

declare(strict_types=1);

/*
 * InMemory backend in tests: same API as Redis and RabbitMQ, no services, and a hand-moved clock that makes delays,
 * retries and expirations instantaneous. The same checks can live in a PHPUnit test of your handlers.
 *
 *   php examples/06-in-memory-tests.php
 */

use QueueBridgeExamples\ManualClock;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Message\Message;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\Retry\FixedDelay;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Worker\ExpiredMessagePolicy;
use snipershady\Queue\Worker\WorkerConfig;

require __DIR__.'/../vendor/autoload.php';
require_once __DIR__.'/Support/ManualClock.php';

$check = static function (bool $condition, string $description): void {
    if (!$condition) {
        throw new RuntimeException('FAILED: '.$description);
    }

    echo "  ok - {$description}\n";
};

$clock = new ManualClock();
// The factory clock stamps createdAt/expiresAt and drives the worker; the DTO clock drives the transport.
$queue = QueueBridge::factory(clock: $clock)->create(new StrategyDtoInMemory($clock), new QueueConfig(name: 'tests', maxAttempts: 2));
$producer = $queue->producer();

$handled = [];
$worker = $queue->worker(new WorkerConfig(
    receiveTimeoutMs: 0, // the InMemory transport never waits
    stopWhenEmpty: true,
    retryStrategy: new FixedDelay(30_000),
    expiredMessagePolicy: ExpiredMessagePolicy::DeadLetter,
))->on('mail', static function (Message $message) use (&$handled): void {
    if ('flaky' === $message->text) {
        throw new RetryableException('SMTP unavailable.');
    }

    $handled[] = $message->text;
});

echo "Delayed message\n";
$producer->publish('mail', 'welcome', delayMs: 60_000);
$worker->run();
$check([] === $handled, 'not delivered before the delay');
$clock->advance(60_000);
$worker->run();
$check(['welcome'] === $handled, 'delivered once the delay has elapsed');

echo "Retries and dead letter queue\n";
$producer->publish('mail', 'flaky');
$worker->run();
$check(1 === $queue->stats()?->delayed, 'first failure: scheduled for a retry in 30 s');
$clock->advance(30_000);
$worker->run();
$check(1 === $queue->stats()?->deadLetters, 'second failure: maxAttempts reached, moved to the DLQ');
$deadLetter = $queue->deadLetters()?->deadLetters()[0] ?? null;
$check('handler_failed' === $deadLetter?->reason->kind->value, 'the DLQ keeps the failure reason');

echo "Expiration\n";
$producer->publish('mail', 'flash sale', ttlMs: 5_000);
$clock->advance(10_000);
$worker->run();
$check(!in_array('flash sale', $handled, strict: true), 'an expired message is not handled');
$check(2 === $queue->stats()?->deadLetters, 'with ExpiredMessagePolicy::DeadLetter it goes to the DLQ');

echo "Requeue from the DLQ\n";
$check(2 === $queue->deadLetters()?->requeueDeadLetters(), 'both dead letters requeued with attempts = 0');
