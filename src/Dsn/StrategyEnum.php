<?php

declare(strict_types=1);

namespace snipershady\Queue\Dsn;

use InvalidArgumentException;

/**
 * Backend indicato da un DSN o dall'opzione `--strategy` della CLI. Non fa parte della firma del factory:
 * serve solo a scegliere il DTO da una configurazione testuale.
 *
 * @api
 */
enum StrategyEnum: string
{
    case Redis = 'redis';
    case RabbitMq = 'rabbitmq';
    case InMemory = 'in-memory';

    /**
     * @throws InvalidArgumentException se lo schema non è supportato
     */
    public static function fromScheme(string $scheme): self
    {
        return match (strtolower($scheme)) {
            'redis', 'rediss' => self::Redis,
            'amqp', 'amqps' => self::RabbitMq,
            'memory' => self::InMemory,
            default => throw new InvalidArgumentException(\sprintf('Unsupported DSN scheme "%s": expected redis, rediss, amqp, amqps or memory.', $scheme)),
        };
    }
}
