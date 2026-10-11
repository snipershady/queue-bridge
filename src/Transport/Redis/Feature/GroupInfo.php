<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\RedisKeys;
use QueueBridge\Transport\Redis\RedisReply;

/**
 * Il gruppo della coda in `XINFO GROUPS`, come mappa campo → valore.
 *
 * @internal
 */
final class GroupInfo
{
    /**
     * Complessità: O(g), g = consumer group dello stream; un round trip.
     *
     * @throws CommandFailedException
     *
     * @return ?array<string, mixed> null se lo stream o il gruppo non esistono ancora
     */
    public static function find(RedisClientInterface $redisClient, RedisKeys $redisKeys): ?array
    {
        try {
            $groups = $redisClient->execute(['XINFO', 'GROUPS', $redisKeys->stream()]);
        } catch (CommandFailedException $commandFailedException) {
            if (\str_contains(strtolower($commandFailedException->getMessage()), 'no such key')) {
                return null;
            }

            throw $commandFailedException;
        }

        foreach (RedisReply::list($groups, 'XINFO GROUPS') as $group) {
            $info = RedisReply::pairs($group, 'XINFO GROUPS entry');
            if (($info['name'] ?? null) === $redisKeys->group()) {
                return $info;
            }
        }

        return null;
    }
}
