<?php

declare(strict_types=1);

/*
 * Manual consumption without the Worker and without the CLI: receive, decode and decide (ack, retry, dead letter)
 * in your own loop. Useful to embed the queue in an existing daemon or event loop. The messages are the lines of
 * examples/data/messages.txt.
 *
 *   php examples/07-manual-consume.php [memory|redis|predis|rabbitmq]
 *
 * With your own loop you take over what the Worker does for you: limits, signals, maxAttempts/maxDeliveries checks,
 * expiration and maintenance (call maintain() periodically on Redis to recover orphans and promote delayed messages).
 */

use QueueBridgeExamples\Backend;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\QueueBridge;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Text\TextKind;
use snipershady\Queue\Transport\Capability\MaintenanceAwareInterface;

require __DIR__.'/../vendor/autoload.php';
require_once __DIR__.'/Support/Backend.php';

$config = new QueueConfig(name: 'example-manual', prefix: 'examples', maxAttempts: 2);
$queue = QueueBridge::factory()->create(Backend::fromCommandLine(), $config);
$queue->setup();

$lines = file(__DIR__.'/data/messages.txt', FILE_IGNORE_NEW_LINES);
if (false === $lines) {
    throw new RuntimeException('Cannot read data/messages.txt.');
}

foreach ($lines as $line) {
    $queue->producer()->publish('events', $line);
}

printf("Published %d messages on %s.\n", count($lines), $queue->transport()::class);

$transport = $queue->transport();
$decoder = new TextDecoder(maxDepth: 32);
$clock = new SystemClock();

$deadLetter = static function (Delivery $delivery, string $why) use ($transport, $clock): void {
    $transport->reject($delivery, new FailureReason(FailureKind::Unrecoverable, $delivery->message->id, $clock->now(), $why));
    printf("  -> dead letter: %s\n", $why);
};

while (true) {
    if ($transport instanceof MaintenanceAwareInterface) {
        $transport->maintain(); // Redis: delayed messages and orphans; a no-op elsewhere
    }

    $deliveries = $transport->receive(max: 5, timeoutMs: 1_000);
    if ([] === $deliveries) {
        if (0 === ($queue->stats()->delayed ?? 0)) {
            break; // nothing ready and nothing waiting for a retry
        }

        usleep(100_000); // the InMemory transport does not wait inside receive()

        continue;
    }

    foreach ($deliveries as $delivery) {
        $message = $delivery->message;
        // Never throws: valid JSON (objects as arrays) or the raw text.
        $decoded = $decoder->decode($message->text);
        printf("%s [%s] %s\n", $message->id->value, $decoded->kind->value, $decoded->raw);

        if (TextKind::Plain === $decoded->kind) {
            printf("  plain text, %d bytes\n", strlen($decoded->raw));
            $transport->ack($delivery);

            continue;
        }

        $event = $decoded->value;
        if (!is_array($event)) {
            printf("  JSON scalar of type %s\n", get_debug_type($event));
            $transport->ack($delivery);

            continue;
        }

        $type = $event['type'] ?? null;
        if (!is_string($type)) {
            $deadLetter($delivery, 'JSON event without a "type".');

            continue;
        }

        switch ($type) {
            case 'user.registered':
                $user = is_array($event['user'] ?? null) ? $event['user'] : [];
                printf("  new user #%s <%s>\n", json_encode($user['id'] ?? null, JSON_THROW_ON_ERROR), json_encode($user['email'] ?? null, JSON_THROW_ON_ERROR));
                $transport->ack($delivery);

                break;

            case 'invoice.paid':
                if ($message->attempts < 1) {
                    // Transient failure: a new copy with attempts + 1 comes back after the delay.
                    printf("  ledger unavailable, retry in 0.5 s\n");
                    $transport->retry($delivery, 500);

                    break;
                }

                printf("  invoice %s paid (attempt %d)\n", json_encode($event['invoice'] ?? null, JSON_THROW_ON_ERROR), $message->attempts + 1);
                $transport->ack($delivery);

                break;

            default:
                $deadLetter($delivery, sprintf('Unknown event type "%s".', $type));
        }
    }
}

$deadLetters = $queue->deadLetters();
printf("Done. Dead letters: %d; purged.\n", count($deadLetters?->deadLetters() ?? []));
$deadLetters?->purgeDeadLetters();
