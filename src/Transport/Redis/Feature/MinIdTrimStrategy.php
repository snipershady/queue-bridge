<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\RedisKeys;
use snipershady\Queue\Transport\Redis\RedisReply;

/**
 * Redis >= 6.2: `XTRIM MINID` con soglia = la più vecchia tra l'entry pendente più vecchia e la successiva
 * all'ultima consegnata: sotto quella soglia ci sono solo entry consegnate e confermate.
 *
 * @internal
 */
final readonly class MinIdTrimStrategy implements TrimStrategyInterface
{
    #[Override]
    public function trim(RedisClientInterface $redisClient, RedisKeys $redisKeys): int
    {
        $group = GroupInfo::find($redisClient, $redisKeys);
        if (null === $group) {
            return 0;
        }

        $lastDelivered = RedisReply::string($group['last-delivered-id'] ?? null, 'XINFO GROUPS last-delivered-id');
        if ('0-0' === $lastDelivered) {
            return 0;
        }

        $summary = RedisReply::list($redisClient->execute(['XPENDING', $redisKeys->stream(), $redisKeys->group()]), 'XPENDING summary');
        $threshold = RedisReply::int($summary[0] ?? null, 'XPENDING count') > 0
            ? RedisReply::string($summary[1] ?? null, 'XPENDING smallest id')
            : $this->next($lastDelivered);

        return RedisReply::int($redisClient->execute(['XTRIM', $redisKeys->stream(), 'MINID', $threshold]), 'XTRIM');
    }

    /**
     * @throws CommandFailedException
     */
    private function next(string $id): string
    {
        if (1 !== \preg_match('/^(\d+)-(\d+)$/D', $id, $matches)) {
            throw CommandFailedException::unexpectedReply('stream id', 'ms-seq');
        }

        return $matches[1].'-'.((int) $matches[2] + 1);
    }
}
