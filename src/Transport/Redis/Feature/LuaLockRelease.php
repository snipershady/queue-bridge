<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use Override;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;

/**
 * Redis >= 6.0: GET + DEL in uno script Lua.
 *
 * @internal
 */
final readonly class LuaLockRelease implements LockReleaseStrategyInterface
{
    /**
     * Complessità: O(1), un round trip (due se lo script non è in cache).
     */
    #[Override]
    public function release(RedisClientInterface $redisClient, string $key, string $token): void
    {
        (new ScriptRunner($redisClient))->run(RedisScripts::releaseLock(), [$key], [$token]);
    }
}
