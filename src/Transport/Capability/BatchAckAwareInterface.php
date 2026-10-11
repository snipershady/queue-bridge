<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Capability;

use QueueBridge\Exception\BackendException;
use QueueBridge\Message\Delivery;

/**
 * @api
 */
interface BatchAckAwareInterface
{
    /**
     * Conferma più messaggi con un solo round trip, dove il backend lo consente.
     *
     * Complessità: O(n), n = consegne; un round trip su Redis, nessuna attesa su RabbitMQ.
     *
     * @param list<Delivery> $deliveries
     *
     * @throws BackendException
     */
    public function ackBatch(array $deliveries): void;
}
