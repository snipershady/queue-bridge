<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Message\Delivery;

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
