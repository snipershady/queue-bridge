<?php

declare(strict_types=1);

/*
 * Benchmark di publish e consume (msg/s) su un backend indicato da DSN.
 *
 *   php tests/Benchmark/benchmark.php <dsn> [messaggi=20000] [batch=100]
 *
 * Esempi (servizi di docker-compose.yml, mai su istanze con dati reali: le code create non vengono rimosse):
 *   php tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0'
 *   php tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0?version=6.0'   # strategie GE60 su un server recente
 *   php tests/Benchmark/benchmark.php 'redis://127.0.0.1:16379/0?client=predis'
 *   php tests/Benchmark/benchmark.php 'amqp://guest:guest@127.0.0.1:35672'
 *   php tests/Benchmark/benchmark.php 'memory://'
 */

use QueueBridge\Cli\CommandLine;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Dsn\StrategyDtoFactory;
use QueueBridge\Message\OutgoingMessage;
use QueueBridge\QueueBridge;
use QueueBridge\Worker\WorkerConfig;

require dirname(__DIR__, 2).'/vendor/autoload.php';

$arguments = CommandLine::arguments();
$dsn = $arguments[0] ?? null;
if (!is_string($dsn)) {
    fwrite(\STDERR, "Usage: php tests/Benchmark/benchmark.php <dsn> [messages=20000] [batch=100]\n");
    exit(2);
}

$messages = max(1, (int) ($arguments[1] ?? 20000));
$batch = max(1, (int) ($arguments[2] ?? 100));
$text = json_encode(['order' => 12345, 'items' => [['sku' => 'A-1', 'qty' => 2]], 'note' => str_repeat('x', 100)], \JSON_THROW_ON_ERROR);

$strategy = (new StrategyDtoFactory())->fromDsn($dsn);
$queue = QueueBridge::factory()->create(
    $strategy,
    new QueueConfig(name: 'bench-'.bin2hex(random_bytes(4)), prefix: 'bench'),
);
$queue->setup();
$producer = $queue->producer();

/**
 * @param Closure(): void $run
 */
$measure = static function (string $label, int $count, Closure $run): void {
    $start = hrtime(as_number: true);
    $run();
    $seconds = (hrtime(as_number: true) - $start) / 1e9;
    printf("%-26s %8d msg in %6.2f s  %10s msg/s\n", $label, $count, $seconds, number_format($count / $seconds));
};

printf("Backend: %s, payload %d byte, batch %d\n", $strategy->name(), strlen($text), $batch);

$single = min($messages, 5000);
$measure('publish (single)', $single, static function () use ($producer, $single, $text): void {
    for ($i = 0; $i < $single; ++$i) {
        $producer->publish('bench', $text);
    }
});

$measure('publishBatch', $messages, static function () use ($producer, $messages, $batch, $text): void {
    for ($sent = 0; $sent < $messages; $sent += $batch) {
        $producer->publishBatch(array_fill(0, min($batch, $messages - $sent), new OutgoingMessage('bench', $text)));
    }
});

$total = $single + $messages;
$handled = 0;
$worker = $queue->worker(new WorkerConfig(batchSize: $batch, receiveTimeoutMs: 200, stopWhenEmpty: true))
    ->on('bench', static function () use (&$handled): void {
        ++$handled;
    });

$measure('consume (handler + ack)', $total, static function () use ($worker): void {
    $worker->run();
});

if ($handled !== $total) {
    fwrite(\STDERR, sprintf("Consumed %d of %d messages.\n", $handled, $total));
    exit(1);
}
