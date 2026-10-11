<?php

declare(strict_types=1);

use QueueBridge\Message\Message;
use QueueBridge\Worker\WorkerInterface;

/*
 * Bootstrap di esempio per `queue-worker work`: registra gli handler sul worker.
 */
return static function (WorkerInterface $worker): void {
    $worker->on('test', static function (Message $message): void {
        fwrite(\STDERR, '');
    });
};
