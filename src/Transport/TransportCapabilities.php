<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

/**
 * Funzionalità offerte da un transport: il core adatta il proprio comportamento, senza mai fallire
 * per una funzionalità assente nella API di base.
 *
 * @api
 */
final readonly class TransportCapabilities
{
    /**
     * @param bool $nativeDelay   i differiti diventano disponibili senza `maintain()`
     * @param bool $batchReceive  `receive()` restituisce più messaggi per chiamata
     * @param bool $batchAck      implementa `BatchAckAwareInterface`
     * @param bool $deadLetter    implementa `DeadLetterAwareInterface`
     * @param bool $consumerLag   `QueueStats::$lag` è disponibile
     * @param bool $deliveryCount `Delivery::$deliveryCount` è affidabile (necessario per `maxDeliveries`)
     */
    public function __construct(
        public OrphanRecovery $orphanRecovery,
        public bool $nativeDelay = false,
        public bool $batchReceive = false,
        public bool $batchAck = false,
        public bool $deadLetter = false,
        public bool $consumerLag = false,
        public bool $deliveryCount = false,
    ) {
    }
}
