<?php

declare(strict_types=1);

namespace QueueBridge;

use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use QueueBridge\Factory\QueueFactory;
use QueueBridge\Producer\ProducerMiddlewareInterface;
use QueueBridge\System\NativeMemoryMeter;
use QueueBridge\System\NullSignalHandler;
use QueueBridge\System\PcntlSignalHandler;
use QueueBridge\System\SystemClock;
use QueueBridge\Transport\InMemory\InMemoryTransportBuilder;
use QueueBridge\Transport\RabbitMq\RabbitMqTransportBuilder;
use QueueBridge\Transport\Redis\RedisTransportBuilder;

/**
 * Composition root: l'unico punto che conosce i backend forniti.
 *
 *     $queue = QueueBridge::factory($logger)->create(StrategyDtoRedis::fromPhpRedis($redis), new QueueConfig(name: 'messages'));
 *
 * @api
 */
final class QueueBridge
{
    /**
     * QueueFactory con i builder di InMemory, Redis e RabbitMQ. Il graceful shutdown sui segnali è attivo
     * se `ext-pcntl` è disponibile.
     *
     * Complessità: O(1).
     *
     * @param list<ProducerMiddlewareInterface> $producerMiddlewares
     * @param ?bool                             $signals             null = attivo se ext-pcntl è caricata
     */
    public static function factory(
        LoggerInterface $logger = new NullLogger(),
        ClockInterface $clock = new SystemClock(),
        array $producerMiddlewares = [],
        ?bool $signals = null,
    ): QueueFactory {
        return new QueueFactory(
            builders: [
                new InMemoryTransportBuilder($clock),
                new RedisTransportBuilder($clock, $logger),
                new RabbitMqTransportBuilder($clock, $logger),
            ],
            clock: $clock,
            logger: $logger,
            signalHandler: ($signals ?? \extension_loaded('pcntl')) ? new PcntlSignalHandler() : new NullSignalHandler(),
            memoryMeter: new NativeMemoryMeter(),
            producerMiddlewares: $producerMiddlewares,
        );
    }
}
