<?php

declare(strict_types=1);

namespace snipershady\Queue;

use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Factory\QueueFactory;
use snipershady\Queue\Producer\ProducerMiddlewareInterface;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\PcntlSignalHandler;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Transport\InMemory\InMemoryTransportBuilder;
use snipershady\Queue\Transport\RabbitMq\RabbitMqTransportBuilder;
use snipershady\Queue\Transport\Redis\RedisTransportBuilder;

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
