<?php

declare(strict_types=1);

/*
 * Quick start without external services (InMemory backend):
 *
 *   php examples/01-quick-start.php
 */

use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Message;
use QueueBridge\QueueBridge;
use QueueBridge\Text\DecodedText;
use QueueBridge\Transport\InMemory\StrategyDtoInMemory;
use QueueBridge\Worker\WorkerConfig;

require __DIR__.'/../vendor/autoload.php';

$queue = QueueBridge::factory()->create(new StrategyDtoInMemory(), new QueueConfig(name: 'messages'));
$queue->setup();

$producer = $queue->producer();
$producer->publish('test', '{"order":42,"total":"19.90"}');
$producer->publish('test', 'some json, some data, some text');
$producer->publish('test', 'reminder', delayMs: 60_000);

$queue->worker(new WorkerConfig(receiveTimeoutMs: 0, stopWhenEmpty: true))
    ->on('test', static function (Message $message, DecodedText $text): void {
        // $text->value is the decoded array for valid JSON, otherwise the raw string.
        printf("%s %s: %s\n", $message->id->value, $text->isJson() ? 'json' : 'text', $text->raw);
    })
    ->run();

printf("Delayed messages still waiting: %d\n", $queue->stats()->delayed ?? 0);
