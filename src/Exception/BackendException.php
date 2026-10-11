<?php

declare(strict_types=1);

namespace QueueBridge\Exception;

/**
 * Errore del backend (Redis, RabbitMQ...). Astratta: si lanciano le sottoclassi specifiche.
 *
 * @api
 */
abstract class BackendException extends QueueException
{
}
