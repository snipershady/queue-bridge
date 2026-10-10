<?php

declare(strict_types=1);

namespace snipershady\Queue\Exception;

use RuntimeException;

/**
 * Radice di tutte le eccezioni della libreria: `catch (QueueException)` intercetta ogni errore applicativo.
 * Astratta: si lanciano solo le sottoclassi specifiche (CLAUDE.md §12).
 *
 * @api
 */
abstract class QueueException extends RuntimeException
{
}
