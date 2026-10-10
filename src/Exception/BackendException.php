<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

/**
 * Errore del backend (Redis, RabbitMQ...). Astratta: si lanciano le sottoclassi specifiche.
 *
 * @api
 */
abstract class BackendException extends QueueException
{
}
