<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis;

use InvalidArgumentException;
use Override;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Idempotency\IdempotencyStoreInterface;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\RandomInterface;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Feature\LockReleaseStrategyInterface;

/**
 * Store di idempotenza su Redis: `SET NX PX` per il lock, rilascio condizionato al token (DELEX su
 * Redis >= 8.4, Lua prima). Usabile anche quando il transport è RabbitMQ (CLAUDE.md §9.9).
 *
 * @api
 */
final readonly class RedisIdempotencyStore implements IdempotencyStoreInterface
{
    private const string DONE = 'done';

    private RedisKeys $keys;

    public function __construct(
        private RedisClientInterface $client,
        private LockReleaseStrategyInterface $lockRelease,
        QueueConfig $config = new QueueConfig(),
        private RandomInterface $random = new NativeRandom(),
    ) {
        $this->keys = new RedisKeys($config);
    }

    /**
     * @phpstan-impure
     */
    #[Override]
    public function acquire(MessageId $id, int $ttlMs): ?LockToken
    {
        $this->assertTtl($ttlMs);
        $token = bin2hex($this->random->bytes(16));

        $reply = $this->client->execute(['SET', $this->keys->idempotency($id), $token, 'NX', 'PX', (string) $ttlMs]);

        return null === $reply ? null : new LockToken($id, $token);
    }

    #[Override]
    public function complete(LockToken $token, int $ttlMs): void
    {
        $this->assertTtl($ttlMs);
        $this->client->execute(['SET', $this->keys->idempotency($token->messageId), self::DONE, 'PX', (string) $ttlMs]);
    }

    #[Override]
    public function release(LockToken $token): void
    {
        $this->lockRelease->release($this->client, $this->keys->idempotency($token->messageId), $token->token);
    }

    private function assertTtl(int $ttlMs): void
    {
        if ($ttlMs < 1) {
            throw new InvalidArgumentException(\sprintf('TTL must be >= 1 ms, got %d.', $ttlMs));
        }
    }
}
