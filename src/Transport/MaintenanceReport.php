<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

use InvalidArgumentException;

/**
 * Esito di `MaintenanceAwareInterface::maintain()`.
 *
 * @api
 */
final readonly class MaintenanceReport
{
    /**
     * @param int $promotedDelayed  differiti diventati disponibili
     * @param int $recoveredOrphans messaggi non confermati rimessi in coda
     * @param int $trimmed          entry rimosse dal trim di sicurezza
     *
     * @throws InvalidArgumentException se un contatore è negativo
     */
    public function __construct(
        public int $promotedDelayed = 0,
        public int $recoveredOrphans = 0,
        public int $trimmed = 0,
    ) {
        foreach (['promotedDelayed' => $promotedDelayed, 'recoveredOrphans' => $recoveredOrphans, 'trimmed' => $trimmed] as $name => $value) {
            if ($value < 0) {
                throw new InvalidArgumentException(\sprintf('%s must be >= 0, got %d.', $name, $value));
            }
        }
    }
}
