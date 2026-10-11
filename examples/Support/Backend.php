<?php

declare(strict_types=1);

namespace QueueBridgeExamples;

use InvalidArgumentException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use Predis\Client;
use QueueBridge\Cli\CommandLine;
use QueueBridge\Factory\StrategyDtoInterface;
use QueueBridge\Transport\InMemory\StrategyDtoInMemory;
use QueueBridge\Transport\RabbitMq\StrategyDtoRabbitMq;
use QueueBridge\Transport\Redis\StrategyDtoRedis;
use Redis;

/**
 * Builds the strategy DTO of the backend named on the command line. Connection settings come from environment
 * variables; the defaults are the ports of docker-compose.yml.
 *
 *   memory    no services
 *   redis     ext-redis    REDIS_HOST (127.0.0.1), REDIS_PORT (16379), REDIS_DB (0)
 *   predis    predis       same variables
 *   rabbitmq  php-amqplib  AMQP_HOST (127.0.0.1), AMQP_PORT (35672), AMQP_USER (guest), AMQP_PASSWORD (guest)
 */
final class Backend
{
    /**
     * Backend named by the first command-line argument (default: memory).
     */
    public static function fromCommandLine(): StrategyDtoInterface
    {
        return self::strategy(CommandLine::arguments()[0] ?? 'memory');
    }

    public static function strategy(string $name): StrategyDtoInterface
    {
        return match ($name) {
            'memory' => new StrategyDtoInMemory(),
            'redis' => self::redis(),
            'predis' => StrategyDtoRedis::fromPredis(new Client([
                'host' => self::env('REDIS_HOST', '127.0.0.1'),
                'port' => (int) self::env('REDIS_PORT', '16379'),
                'database' => (int) self::env('REDIS_DB', '0'),
            ])),
            'rabbitmq' => self::rabbitMq(),
            default => throw new InvalidArgumentException(\sprintf('Unknown backend "%s": use memory, redis, predis or rabbitmq.', $name)),
        };
    }

    public static function env(string $name, string $default): string
    {
        $value = getenv($name);

        return \is_string($value) && '' !== $value ? $value : $default;
    }

    private static function redis(): StrategyDtoRedis
    {
        // A factory instead of an instance: the client calls it again to reconnect after a network failure.
        return StrategyDtoRedis::fromPhpRedis(static function (): Redis {
            $redis = new Redis();
            $redis->connect(self::env('REDIS_HOST', '127.0.0.1'), (int) self::env('REDIS_PORT', '16379'), 2.0);
            $redis->select((int) self::env('REDIS_DB', '0'));

            return $redis;
        });
    }

    private static function rabbitMq(): StrategyDtoRabbitMq
    {
        return StrategyDtoRabbitMq::fromConnectionFactory(static fn (): AbstractConnection => new AMQPStreamConnection(
            self::env('AMQP_HOST', '127.0.0.1'),
            (int) self::env('AMQP_PORT', '35672'),
            self::env('AMQP_USER', 'guest'),
            self::env('AMQP_PASSWORD', 'guest'),
        ));
    }
}
