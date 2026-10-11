<?php

declare(strict_types=1);

/*
 * The same application code on every backend: only the DTO passed to create() changes.
 *
 *   php examples/02-switch-backend.php memory
 *   php examples/02-switch-backend.php redis      # ext-redis
 *   php examples/02-switch-backend.php predis     # predis/predis
 *   php examples/02-switch-backend.php rabbitmq   # php-amqplib
 *
 * REDIS_HOST/REDIS_PORT and AMQP_HOST/AMQP_PORT/AMQP_USER/AMQP_PASSWORD select the servers; the defaults are the
 * ports of docker-compose.yml.
 */

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use Predis\Client;
use snipershady\Queue\Cli\CommandLine;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Message\Message;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Transport\RabbitMq\StrategyDtoRabbitMq;
use snipershady\Queue\Transport\Redis\StrategyDtoRedis;
use snipershady\Queue\Worker\WorkerConfig;

require __DIR__.'/../vendor/autoload.php';

$env = static fn (string $name, string $default): string => is_string($value = getenv($name)) && '' !== $value ? $value : $default;

$strategy = match (CommandLine::arguments()[0] ?? 'memory') {
    'redis' => StrategyDtoRedis::fromPhpRedis(static function () use ($env): Redis {
        // A factory: the client calls it again to reconnect after a failure.
        $redis = new Redis();
        $redis->connect($env('REDIS_HOST', '127.0.0.1'), (int) $env('REDIS_PORT', '16379'));

        return $redis;
    }),
    // Predis reconnects on its own: pass the client.
    'predis' => StrategyDtoRedis::fromPredis(new Client(['host' => $env('REDIS_HOST', '127.0.0.1'), 'port' => (int) $env('REDIS_PORT', '16379')])),
    'rabbitmq' => StrategyDtoRabbitMq::fromConnectionFactory(static fn (): AbstractConnection => new AMQPStreamConnection(
        $env('AMQP_HOST', '127.0.0.1'),
        (int) $env('AMQP_PORT', '35672'),
        $env('AMQP_USER', 'guest'),
        $env('AMQP_PASSWORD', 'guest'),
    )),
    default => new StrategyDtoInMemory(),
};

/*
 * From here on nothing depends on the backend.
 */
$application = static function (StrategyDtoInterface $strategy): void {
    $queue = QueueBridge::factory()->create($strategy, new QueueConfig(name: 'example-switch', prefix: 'examples'));
    $queue->setup();

    $queue->producer()->publish('test', '{"hello":"world"}');
    $queue->producer()->publish('test', 'plain text');

    $reason = $queue->worker(new WorkerConfig(receiveTimeoutMs: 500, stopWhenEmpty: true))
        ->on('test', static function (Message $message): void {
            printf("  %s => %s\n", $message->channel, $message->text);
        })
        ->run();

    printf("[%s] worker stopped (%s), %d ready\n", $strategy->name(), $reason->value, $queue->stats()->ready ?? 0);
};

$application($strategy);
