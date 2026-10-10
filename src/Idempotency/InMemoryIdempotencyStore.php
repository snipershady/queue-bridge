<?php

declare(strict_types=1);

namespace snipershady\Queue\Idempotency;

use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\RandomInterface;

/**
 * Store in memoria, valido solo nel processo corrente: per test e worker singoli.
 *
 * @api
 */
final class InMemoryIdempotencyStore implements IdempotencyStoreInterface
{
    private const string DONE = '';

    /** @var array<string, array{token: string, expiresAtMs: int}> token vuoto = completato */
    private array $entries = [];

    public function __construct(
        private readonly ClockInterface $clock,
        private readonly RandomInterface $random = new NativeRandom(),
    ) {
    }

    /**
     * @phpstan-impure
     */
    #[Override]
    public function acquire(MessageId $id, int $ttlMs): ?LockToken
    {
        $this->assertTtl($ttlMs);
        $nowMs = $this->nowMs();
        $entry = $this->entries[$id->value] ?? null;

        if (null !== $entry && $entry['expiresAtMs'] > $nowMs) {
            return null;
        }

        $token = bin2hex($this->random->bytes(16));
        $this->entries[$id->value] = ['token' => $token, 'expiresAtMs' => $nowMs + $ttlMs];

        return new LockToken($id, $token);
    }

    #[Override]
    public function complete(LockToken $token, int $ttlMs): void
    {
        $this->assertTtl($ttlMs);
        $this->entries[$token->messageId->value] = ['token' => self::DONE, 'expiresAtMs' => $this->nowMs() + $ttlMs];
    }

    #[Override]
    public function release(LockToken $token): void
    {
        if (($this->entries[$token->messageId->value]['token'] ?? null) === $token->token) {
            unset($this->entries[$token->messageId->value]);
        }
    }

    private function assertTtl(int $ttlMs): void
    {
        if ($ttlMs < 1) {
            throw new InvalidArgumentException(\sprintf('TTL must be >= 1 ms, got %d.', $ttlMs));
        }
    }

    private function nowMs(): int
    {
        return (int) $this->clock->now()->format('Uv');
    }
}
