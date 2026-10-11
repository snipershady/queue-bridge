<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\RedisKeys;

/**
 * Trim di sicurezza dello stream principale: rimuove solo entry già confermate, mai messaggi da leggere
 * o in elaborazione. Con ack + delete lo stream si svuota da solo: il trim raccoglie
 * soltanto eventuali residui.
 *
 * @internal
 */
interface TrimStrategyInterface
{
    /**
     * Complessità: Un numero costante di round trip; lato server O(e), e = entry rimosse.
     *
     * @throws CommandFailedException
     *
     * @return int entry rimosse
     */
    public function trim(RedisClientInterface $redisClient, RedisKeys $redisKeys): int;
}
