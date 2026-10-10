<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Capability;

use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Transport\MaintenanceReport;

/**
 * @api
 */
interface MaintenanceAwareInterface
{
    /**
     * Promuove i differiti scaduti, recupera i messaggi orfani (oltre `QueueConfig::$claimIdleMs`) ed
     * esegue il trim di sicurezza. Sicuro da chiamare in parallelo da più worker.
     *
     * @throws BackendException
     */
    public function maintain(): MaintenanceReport;
}
