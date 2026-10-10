<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisKeys;

/**
 * Trim di sicurezza dello stream principale: rimuove solo entry già confermate, mai messaggi da leggere
 * o in elaborazione (CLAUDE.md §7.3). Con ack + delete lo stream si svuota da solo: il trim raccoglie
 * soltanto eventuali residui.
 *
 * @internal
 */
interface TrimStrategyInterface
{
    /**
     * @throws CommandFailedException
     *
     * @return int entry rimosse
     */
    public function trim(RedisClientInterface $redisClient, RedisKeys $redisKeys): int;
}
