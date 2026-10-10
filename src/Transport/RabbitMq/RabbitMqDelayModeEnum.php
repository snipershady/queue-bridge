<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq;

/**
 * Come si implementano i differiti su RabbitMQ.
 *
 * @api
 */
enum RabbitMqDelayModeEnum: string
{
    /**
     * Code di attesa a TTL fisso (RetryTiers) che fanno dead-letter verso la coda principale: nessun plugin,
     * nessun head-of-line blocking. Precisione di circa uno scaglione.
     */
    case RetryTiers = 'retry_tiers';
}
