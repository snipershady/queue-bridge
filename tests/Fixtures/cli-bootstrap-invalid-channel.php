<?php

declare(strict_types=1);

use QueueBridge\Worker\WorkerInterface;

/*
 * Bootstrap che registra un canale non valido: l'InvalidArgumentException arriva alla CLI così com'è.
 */
return static function (WorkerInterface $worker): void {
    $worker->on('not a channel!', static function (): void {
    });
};
