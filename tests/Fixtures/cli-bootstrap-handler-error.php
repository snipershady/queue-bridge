<?php

declare(strict_types=1);

use QueueBridge\Worker\WorkerInterface;

/*
 * Bootstrap la cui Closure fallisce mentre registra gli handler.
 */
return static function (WorkerInterface $worker): void {
    throw new RuntimeException('missing service "mailer"');
};
