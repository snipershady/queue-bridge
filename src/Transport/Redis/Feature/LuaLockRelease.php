<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Script\RedisScripts;
use snipershady\Queue\Transport\Redis\Script\ScriptRunner;

/**
 * Redis >= 6.0: GET + DEL in uno script Lua.
 *
 * @internal
 */
final readonly class LuaLockRelease implements LockReleaseStrategyInterface
{
    #[Override]
    public function release(RedisClientInterface $redisClient, string $key, string $token): void
    {
        (new ScriptRunner($redisClient))->run(RedisScripts::releaseLock(), [$key], [$token]);
    }
}
