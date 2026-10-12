<?php

declare(strict_types=1);

namespace QueueBridge\Transport;

/**
 * Come tornano in coda i messaggi ricevuti da un consumer che non li ha confermati (crash).
 *
 * @api
 */
enum OrphanRecovery: string
{
    /**
     * Il backend li rimette in coda da solo (RabbitMQ, alla chiusura del canale).
     */
    case Native = 'native';

    /**
     * Vanno recuperati esplicitamente: con `MaintenanceAwareInterface::maintain()` (InMemory, Redis < 8.4) oppure
     * dal transport stesso in receive() (Redis >= 8.4, XREADGROUP ... CLAIM; maintain() ne riporta 0). In ogni caso
     * solo dopo `claimIdleMs`: per questo il worker limita l'età delle consegne che inizia a elaborare.
     */
    case Explicit = 'explicit';
}
