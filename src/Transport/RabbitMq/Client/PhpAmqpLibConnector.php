<?php

declare(strict_types=1);

namespace QueueBridge\Transport\RabbitMq\Client;

use Closure;
use Override;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use QueueBridge\Exception\ConnectionException;
use Throwable;

/**
 * Connector per `php-amqplib`. Riapre il canale dopo ogni errore; con una factory ricrea anche la
 * connessione quando è caduta. I canali hanno sempre le publisher confirms attive.
 *
 * @api
 */
final class PhpAmqpLibConnector implements AmqpConnectorInterface
{
    private ?AbstractConnection $connection;

    private ?AmqpChannelInterface $channel = null;

    private int $generation = 0;

    /** @var ?Closure(): AbstractConnection */
    private readonly ?Closure $factory;

    /**
     * Complessità: O(1).
     *
     * @param AbstractConnection|Closure(): AbstractConnection $connection connessione aperta, o factory che ne crea una
     */
    public function __construct(AbstractConnection|Closure $connection)
    {
        $this->connection = $connection instanceof AbstractConnection ? $connection : null;
        $this->factory = $connection instanceof Closure ? $connection : null;
    }

    /**
     * Complessità: O(1); apre canale (e connessione) solo se chiusi.
     */
    #[Override]
    public function channel(): AmqpChannelInterface
    {
        if ($this->channel instanceof AmqpChannelInterface && $this->channel->isOpen()) {
            return $this->channel;
        }

        $this->channel = new PhpAmqpLibChannel($this->openChannel());
        ++$this->generation;

        return $this->channel;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function generation(): int
    {
        return $this->generation;
    }

    /**
     * Complessità: O(1), un round trip per chiudere il canale.
     */
    #[Override]
    public function reset(): void
    {
        $this->channel?->close();
        $this->channel = null;
    }

    /**
     * @throws ConnectionException
     */
    private function openChannel(): AMQPChannel
    {
        if ($this->factory instanceof Closure && (!$this->connection instanceof AbstractConnection || !$this->connection->isConnected())) {
            try {
                $this->connection = ($this->factory)();
            } catch (Throwable $throwable) {
                throw ConnectionException::unreachable('RabbitMQ', $throwable->getMessage(), $throwable);
            }
        }

        if (!$this->connection instanceof AbstractConnection || !$this->connection->isConnected()) {
            throw ConnectionException::unreachable('RabbitMQ', 'the connection is closed');
        }

        try {
            return $this->connection->channel();
        } catch (Throwable $throwable) {
            throw ConnectionException::unreachable('RabbitMQ', $throwable->getMessage(), $throwable);
        }
    }
}
