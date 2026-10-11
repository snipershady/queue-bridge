<?php

declare(strict_types=1);

/*
 * CLI bootstrap: registers the handlers on the worker created by `queue-worker work`.
 *
 *   vendor/bin/queue-worker work --dsn='redis://127.0.0.1:16379/0' --queue=messages \
 *       --bootstrap=examples/03-worker-bootstrap.php --max-runtime=3600 --memory-limit=256
 *
 * The supervisor (systemd, supervisord, Kubernetes) restarts the process when it exits; see examples/deploy/.
 */

use QueueBridge\Exception\RetryableException;
use QueueBridge\Exception\UnrecoverableMessageException;
use QueueBridge\Message\Message;
use QueueBridge\Text\DecodedText;
use QueueBridge\Worker\WorkerInterface;

return static function (WorkerInterface $worker): void {
    $worker
        ->on('orders', static function (Message $message, DecodedText $text): void {
            if (!$text->isJson() || !is_array($text->value)) {
                // No retry: the message goes straight to the DLQ.
                throw new UnrecoverableMessageException('Order payload must be a JSON object.');
            }

            if (!isset($text->value['id'])) {
                // Retried with the worker's strategy; delayMs overrides it.
                throw new RetryableException('Order id not available yet.', delayMs: 5_000);
            }

            fwrite(\STDOUT, sprintf("order %s processed (attempt %d)\n", (string) json_encode($text->value['id']), $message->attempts + 1));
        })
        // Special channel "*": receives the channels without a dedicated handler.
        ->on('*', static function (Message $message): void {
            fwrite(\STDOUT, sprintf("ignored message on channel %s\n", $message->channel));
        });
};
