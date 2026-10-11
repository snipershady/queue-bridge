<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use Override;
use QueueBridge\Transport\RabbitMq\Client\AmqpChannelInterface;
use QueueBridge\Transport\RabbitMq\Client\AmqpConnectorInterface;

/**
 * Connector finto: un canale "riaperto" dopo reset() è lo stesso FakeAmqpChannel con una generazione nuova.
 */
final class FakeAmqpConnector implements AmqpConnectorInterface
{
    public int $resets = 0;

    private int $generation = 0;

    private bool $current = false;

    public function __construct(public readonly FakeAmqpChannel $channel = new FakeAmqpChannel())
    {
    }

    #[Override]
    public function channel(): AmqpChannelInterface
    {
        if (!$this->current) {
            $this->current = true;
            $this->channel->open = true;
            ++$this->generation;
        }

        return $this->channel;
    }

    #[Override]
    public function generation(): int
    {
        return $this->generation;
    }

    #[Override]
    public function reset(): void
    {
        ++$this->resets;
        $this->current = false;
    }
}
