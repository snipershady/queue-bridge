<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use Override;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\TransportCapabilities;
use QueueBridge\Transport\TransportInterface;
use RuntimeException;

/**
 * Transport finto che registra ogni operazione; `receive()` restituisce i batch programmati in `$incoming`.
 */
final class RecordingTransport implements TransportInterface
{
    /** @var list<Envelope> */
    public array $sent = [];

    /** @var list<list<Envelope>> */
    public array $batches = [];

    /** @var list<list<Delivery>> */
    public array $incoming = [];

    /** @var list<array{int, int}> max e timeout di ogni receive() */
    public array $receiveCalls = [];

    /** @var list<Delivery> */
    public array $acked = [];

    /** @var list<array{Delivery, int}> */
    public array $retried = [];

    /** @var list<array{Delivery, FailureReason}> */
    public array $rejected = [];

    public ?RuntimeException $failOnAck = null;

    public function __construct(
        public TransportCapabilities $capabilities = new TransportCapabilities(OrphanRecovery::Native, deliveryCount: true),
    ) {
    }

    #[Override]
    public function send(Message $message, int $delayMs = 0): void
    {
        $this->sent[] = new Envelope($message, $delayMs);
    }

    #[Override]
    public function sendBatch(iterable $envelopes): void
    {
        $batch = [];
        foreach ($envelopes as $envelope) {
            $batch[] = $envelope;
        }

        $this->batches[] = $batch;
    }

    #[Override]
    public function receive(int $max = 1, int $timeoutMs = 1000): array
    {
        $this->receiveCalls[] = [$max, $timeoutMs];

        return array_shift($this->incoming) ?? [];
    }

    #[Override]
    public function ack(Delivery $delivery): void
    {
        if ($this->failOnAck instanceof RuntimeException) {
            throw $this->failOnAck;
        }

        $this->acked[] = $delivery;
    }

    #[Override]
    public function retry(Delivery $delivery, int $delayMs): void
    {
        $this->retried[] = [$delivery, $delayMs];
    }

    #[Override]
    public function reject(Delivery $delivery, FailureReason $reason): void
    {
        $this->rejected[] = [$delivery, $reason];
    }

    #[Override]
    public function release(Delivery $delivery): void
    {
    }

    #[Override]
    public function ping(): void
    {
    }

    #[Override]
    public function capabilities(): TransportCapabilities
    {
        return $this->capabilities;
    }
}
