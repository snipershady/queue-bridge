<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq\Client;

use Closure;
use Override;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPEmptyDeliveryTagException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage as NativeMessage;
use PhpAmqpLib\Wire\AMQPTable;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;
use Throwable;

/**
 * Adapter per un canale `php-amqplib`. Con le publisher confirms attive, confirm() attende ack/nack del
 * broker; un messaggio restituito (non instradabile) conta come fallito anche se poi viene confermato.
 *
 * @api
 */
final class PhpAmqpLibChannel implements AmqpChannelInterface
{
    /** @var list<string> */
    private array $failed = [];

    public function __construct(
        private readonly AMQPChannel $channel,
        private readonly bool $publisherConfirms = true,
    ) {
        if (!$publisherConfirms) {
            return;
        }

        $this->guard('confirm.select', static fn () => $channel->confirm_select());
        $channel->set_nack_handler(function (NativeMessage $nativeMessage): void {
            $this->failed[] = self::messageId($nativeMessage);
        });
        $channel->set_return_listener(function (mixed $replyCode, mixed $replyText, mixed $exchange, mixed $routingKey, NativeMessage $nativeMessage): void {
            $this->failed[] = self::messageId($nativeMessage);
        });
    }

    #[Override]
    public function declareExchange(string $name, string $type, array $arguments = []): void
    {
        $this->guard('exchange.declare', fn () => $this->channel->exchange_declare($name, $type, passive: false, durable: true, auto_delete: false, internal: false, nowait: false, arguments: new AMQPTable($arguments)));
    }

    #[Override]
    public function declareQueue(string $name, array $arguments = [], bool $passive = false): array
    {
        $reply = $this->guard('queue.declare', fn () => $this->channel->queue_declare($name, $passive, durable: true, exclusive: false, auto_delete: false, nowait: false, arguments: new AMQPTable($arguments)));

        if (!\is_array($reply)) {
            return [0, 0];
        }

        $messages = $reply[1] ?? 0;
        $consumers = $reply[2] ?? 0;

        return [\is_int($messages) ? $messages : 0, \is_int($consumers) ? $consumers : 0];
    }

    #[Override]
    public function bindQueue(string $queue, string $exchange, string $routingKey): void
    {
        $this->guard('queue.bind', fn () => $this->channel->queue_bind($queue, $exchange, $routingKey));
    }

    #[Override]
    public function purgeQueue(string $queue): int
    {
        $purged = $this->guard('queue.purge', fn () => $this->channel->queue_purge($queue));

        return \is_int($purged) ? $purged : 0;
    }

    #[Override]
    public function prefetch(int $count): void
    {
        $this->guard('basic.qos', fn () => $this->channel->basic_qos(0, $count, a_global: false));
    }

    #[Override]
    public function consume(string $queue, Closure $onMessage): void
    {
        $this->guard('basic.consume', fn () => $this->channel->basic_consume($queue, '', no_local: false, no_ack: false, exclusive: false, nowait: false, callback: static function (NativeMessage $nativeMessage) use ($onMessage): void {
            $onMessage(self::toMessage($nativeMessage));
        }));
    }

    #[Override]
    public function wait(int $timeoutMs): bool
    {
        try {
            $this->guard('wait', fn () => $this->channel->wait(non_blocking: 0 === $timeoutMs, timeout: $timeoutMs / 1000));
        } catch (ConnectionException $connectionException) {
            if ($connectionException->getPrevious() instanceof AMQPTimeoutException) {
                return false;
            }

            throw $connectionException;
        }

        return true;
    }

    #[Override]
    public function get(string $queue): ?AmqpMessage
    {
        $nativeMessage = $this->guard('basic.get', fn () => $this->channel->basic_get($queue));

        return $nativeMessage instanceof NativeMessage ? self::toMessage($nativeMessage) : null;
    }

    #[Override]
    public function publish(AmqpMessage $amqpMessage, string $exchange, string $routingKey): void
    {
        $properties = ['delivery_mode' => NativeMessage::DELIVERY_MODE_PERSISTENT, 'application_headers' => new AMQPTable($amqpMessage->headers)];
        if (null !== $amqpMessage->messageId) {
            $properties['message_id'] = $amqpMessage->messageId;
        }

        $this->guard('basic.publish', fn () => $this->channel->basic_publish(new NativeMessage($amqpMessage->body, $properties), $exchange, $routingKey, mandatory: true));
    }

    #[Override]
    public function confirm(int $timeoutMs): array
    {
        if ($this->publisherConfirms) {
            $this->guard('confirm', fn () => $this->channel->wait_for_pending_acks_returns($timeoutMs / 1000));
        }

        $failed = $this->failed;
        $this->failed = [];

        return $failed;
    }

    #[Override]
    public function ack(int $deliveryTag): void
    {
        $this->guard('basic.ack', fn () => $this->channel->basic_ack($deliveryTag));
    }

    #[Override]
    public function nack(int $deliveryTag, bool $requeue): void
    {
        $this->guard('basic.nack', fn () => $this->channel->basic_nack($deliveryTag, multiple: false, requeue: $requeue));
    }

    #[Override]
    public function isOpen(): bool
    {
        return $this->channel->is_open();
    }

    #[Override]
    public function close(): void
    {
        try {
            $this->channel->close();
        } catch (Throwable) {
            // Canale già chiuso dal broker o connessione persa: non c'è altro da chiudere.
        }
    }

    /**
     * Errori di protocollo del canale (coda inesistente, argomenti diversi) → CommandFailedException;
     * tutto il resto (I/O, connessione chiusa, timeout) → ConnectionException.
     *
     * @template T
     *
     * @param Closure(): T $operation
     *
     * @throws CommandFailedException|ConnectionException
     *
     * @return T
     */
    private function guard(string $method, Closure $operation): mixed
    {
        try {
            return $operation();
        } catch (AMQPProtocolChannelException $amqpProtocolChannelException) {
            throw CommandFailedException::rejected($method, $amqpProtocolChannelException->getMessage(), $amqpProtocolChannelException);
        } catch (Throwable $throwable) {
            throw ConnectionException::unreachable('RabbitMQ', $throwable->getMessage(), $throwable);
        }
    }

    private static function toMessage(NativeMessage $nativeMessage): AmqpMessage
    {
        $headers = [];
        $table = $nativeMessage->has('application_headers') ? $nativeMessage->get('application_headers') : null;
        if ($table instanceof AMQPTable) {
            foreach ($table->getNativeData() as $name => $value) {
                if (\is_string($value) || \is_int($value)) {
                    $headers[(string) $name] = $value;
                }
            }
        }

        try {
            $deliveryTag = $nativeMessage->getDeliveryTag();
        } catch (AMQPEmptyDeliveryTagException) {
            // Messaggio non ancora consegnato (es. restituito dal broker durante una publish).
            $deliveryTag = null;
        }

        return new AmqpMessage($nativeMessage->getBody(), $headers, self::messageId($nativeMessage), $deliveryTag);
    }

    private static function messageId(NativeMessage $nativeMessage): string
    {
        $messageId = $nativeMessage->has('message_id') ? $nativeMessage->get('message_id') : '';

        return \is_string($messageId) ? $messageId : '';
    }
}
