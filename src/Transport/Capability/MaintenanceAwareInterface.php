<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Capability;

use QueueBridge\Exception\BackendException;
use QueueBridge\Transport\MaintenanceReport;

/**
 * @api
 */
interface MaintenanceAwareInterface
{
    /**
     * Promuove i differiti scaduti, recupera i messaggi orfani (oltre `QueueConfig::$claimIdleMs`) ed
     * esegue il trim di sicurezza. Sicuro da chiamare in parallelo da più worker.
     *
     * Complessità: O(p + o), p = differiti scaduti, o = orfani (Redis: al più 1000 orfani per chiamata).
     *
     * @throws BackendException
     */
    public function maintain(): MaintenanceReport;
}
