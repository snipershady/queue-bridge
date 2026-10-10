<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq;

use Closure;
use InvalidArgumentException;
use Override;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpConnectorInterface;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibConnector;

/**
 * Backend RabbitMQ. Il DTO non apre canali: li apre (e li riapre) il connector al primo uso.
 *
 * @api
 */
final readonly class StrategyDtoRabbitMq implements StrategyDtoInterface
{
    /**
     * @param int $prefetch messaggi consegnati in anticipo a questo consumer
     *
     * @throws InvalidArgumentException se $prefetch < 1
     */
    public function __construct(
        public AmqpConnectorInterface $connector,
        public RabbitMqDelayModeEnum $delayMode = RabbitMqDelayModeEnum::RetryTiers,
        public RetryTiers $retryTiers = new RetryTiers(),
        public int $prefetch = 10,
    ) {
        if ($prefetch < 1) {
            throw new InvalidArgumentException(\sprintf('Prefetch must be >= 1, got %d.', $prefetch));
        }
    }

    /**
     * Usa la connessione del canale; i canali della coda vengono aperti su di essa.
     *
     * @throws InvalidArgumentException se il canale non ha più una connessione
     */
    public static function fromPhpAmqpLib(AMQPChannel $channel, bool $publisherConfirms = true): self
    {
        $connection = $channel->getConnection();
        if (!$connection instanceof AbstractConnection) {
            throw new InvalidArgumentException('The AMQP channel is not attached to a connection.');
        }

        return self::fromConnection($connection, $publisherConfirms);
    }

    public static function fromConnection(AbstractConnection $connection, bool $publisherConfirms = true): self
    {
        return new self(new PhpAmqpLibConnector($connection, $publisherConfirms));
    }

    /**
     * Riconnessione completa (connessione e canale) dopo una caduta.
     *
     * @param Closure(): AbstractConnection $factory
     */
    public static function fromConnectionFactory(Closure $factory, bool $publisherConfirms = true): self
    {
        return new self(new PhpAmqpLibConnector($factory, $publisherConfirms));
    }

    #[Override]
    public function name(): string
    {
        return 'rabbitmq';
    }
}
