<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

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
     * Vanno recuperati con `MaintenanceAwareInterface::maintain()` (Redis, InMemory).
     */
    case Explicit = 'explicit';
}
