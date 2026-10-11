<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Transport\QueuedMessage;

/**
 * Elenco dei messaggi in coda senza consumarli: attempts e ID non cambiano.
 *
 * Ordine: in elaborazione, poi pronti nell'ordine di consegna, poi differiti per scadenza. È un'istantanea:
 * con worker attivi i messaggi possono cambiare stato mentre si legge. Le entry non decodificabili vengono
 * saltate. Su RabbitMQ vedi la nota del transport (solo messaggi pronti, letti con basic.get).
 *
 * @api
 */
interface BrowseAwareInterface
{
    /**
     * Complessità: O(limit) messaggi decodificati: un numero costante di round trip su Redis, limit + 1 su RabbitMQ.
     *
     * @throws InvalidArgumentException se $limit < 1
     * @throws BackendException
     *
     * @return list<QueuedMessage> al più $limit messaggi
     */
    public function browse(int $limit = 100): array;
}
