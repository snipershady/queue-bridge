<?php

declare(strict_types=1);

namespace QueueBridge\Transport\RabbitMq\Client;

/**
 * Messaggio AMQP indipendente dalla libreria client. Header con valori scalari: quelli strutturati
 * (es. `x-death`) non vengono esposti.
 *
 * @api
 */
final readonly class AmqpMessage
{
    /**
     * Complessità: O(1).
     *
     * @param array<string, string|int> $headers
     * @param ?int                      $deliveryTag presente solo nei messaggi ricevuti
     */
    public function __construct(
        public string $body,
        public array $headers = [],
        public ?string $messageId = null,
        public ?int $deliveryTag = null,
    ) {
    }
}
