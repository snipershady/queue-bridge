<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisKeys;
use snipershady\Queue\Transport\Redis\RedisReply;

/**
 * Redis >= 8.2: `XTRIM key MAXLEN 0 ACKED` rimuove solo entry confermate da tutti i gruppi.
 *
 * @internal
 */
final readonly class AckedTrimStrategy implements TrimStrategyInterface
{
    /**
     * Complessità: O(1) round trip; lato server O(e), e = entry rimosse.
     */
    #[Override]
    public function trim(RedisClientInterface $redisClient, RedisKeys $redisKeys): int
    {
        return RedisReply::int($redisClient->execute(['XTRIM', $redisKeys->stream(), 'MAXLEN', '0', 'ACKED']), 'XTRIM');
    }
}
