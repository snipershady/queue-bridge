<?php

declare(strict_types=1);

namespace QueueBridge\Exception;

use RuntimeException;

/**
 * Radice di tutte le eccezioni della libreria: `catch (QueueException)` intercetta ogni errore applicativo.
 * Astratta: si lanciano solo le sottoclassi specifiche.
 *
 * @api
 */
abstract class QueueException extends RuntimeException
{
}
