<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Transport\Redis\RedisReply;

/**
 * Risposta di XPENDING esteso: [[id, consumer, inattività ms, consegne], ...].
 *
 * @internal
 */
final class PendingEntries
{
    /**
     * Complessità: O(n), n = voci della risposta di XPENDING.
     *
     * @throws CommandFailedException
     *
     * @return list<string>
     */
    public static function idleIds(mixed $reply, int $minIdleMs): array
    {
        $ids = [];
        foreach (RedisReply::list($reply, 'XPENDING') as $entry) {
            $entry = RedisReply::list($entry, 'XPENDING entry');
            if (RedisReply::int($entry[2] ?? null, 'XPENDING idle') >= $minIdleMs) {
                $ids[] = RedisReply::string($entry[0] ?? null, 'XPENDING id');
            }
        }

        return $ids;
    }
}
