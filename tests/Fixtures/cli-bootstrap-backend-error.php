<?php

declare(strict_types=1);

use QueueBridge\Exception\ConnectionException;
use QueueBridge\Worker\WorkerInterface;

/*
 * Bootstrap la cui Closure incontra un errore del backend: exit code 1, come gli altri errori del backend.
 */
return static function (WorkerInterface $worker): void {
    throw ConnectionException::unreachable('Test', 'down');
};
