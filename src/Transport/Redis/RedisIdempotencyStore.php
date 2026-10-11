<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis;

use InvalidArgumentException;
use Override;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\VersionMismatchException;
use QueueBridge\Idempotency\AcquireResult;
use QueueBridge\Idempotency\IdempotencyStoreInterface;
use QueueBridge\Idempotency\LockToken;
use QueueBridge\Message\MessageId;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\RandomInterface;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\Feature\LockReleaseStrategyInterface;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;
use QueueBridge\Transport\Redis\Version\RedisCapabilities;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;
use QueueBridge\Transport\Redis\Version\VersionDetector;

/**
 * Store di idempotenza su Redis: `SET NX PX` per il lock, in uno script che distingue "completato" da
 * "in elaborazione"; rilascio condizionato al token (DELEX su Redis >= 8.4, Lua prima). Usabile anche
 * quando il transport è RabbitMQ.
 *
 * @api
 */
final readonly class RedisIdempotencyStore implements IdempotencyStoreInterface
{
    private const string DONE = 'done';

    private const int ACQUIRED = 1;

    private const int COMPLETED = 2;

    private RedisKeys $keys;

    private ScriptRunner $scripts;

    /**
     * Complessità: O(1).
     */
    public function __construct(
        private RedisClientInterface $client,
        private LockReleaseStrategyInterface $lockRelease,
        QueueConfig $config = new QueueConfig(),
        private RandomInterface $random = new NativeRandom(),
    ) {
        $this->keys = new RedisKeys($config);
        $this->scripts = new ScriptRunner($client);
    }

    /**
     * Sceglie il rilascio del lock in base alla versione del server (DELEX su Redis >= 8.4, Lua prima).
     *
     * Complessità: O(1), un round trip (INFO server) se il livello non è forzato.
     *
     * @throws VersionMismatchException se il livello forzato supera la versione del server
     */
    public static function create(
        RedisClientInterface $client,
        QueueConfig $config = new QueueConfig(),
        ?RedisVersionEnum $version = null,
        LoggerInterface $logger = new NullLogger(),
    ): self {
        $level = (new VersionDetector($client, $logger))->resolve($version);

        return new self($client, FeatureSelector::select(RedisCapabilities::forVersion($level))->lockRelease, $config);
    }

    /**
     * Complessità: O(1), un round trip (due se lo script non è in cache).
     *
     * @phpstan-impure
     */
    #[Override]
    public function acquire(MessageId $id, int $ttlMs): AcquireResult
    {
        $this->assertTtl($ttlMs);
        $token = bin2hex($this->random->bytes(16));

        $reply = RedisReply::list(
            $this->scripts->run(RedisScripts::acquireLock(), [$this->keys->idempotency($id)], [$token, (string) $ttlMs, self::DONE]),
            'acquireLock',
        );

        return match (RedisReply::int($reply[0] ?? null, 'acquireLock status')) {
            self::ACQUIRED => AcquireResult::acquired(new LockToken($id, $token)),
            self::COMPLETED => AcquireResult::completed(),
            // PTTL < 1: il lock è scaduto proprio ora (o è senza scadenza): conviene comunque riprovare presto.
            default => AcquireResult::inProgress(\max(1, RedisReply::int($reply[1] ?? null, 'acquireLock pttl'))),
        };
    }

    /**
     * Complessità: O(1), un round trip.
     */
    #[Override]
    public function complete(LockToken $token, int $ttlMs): void
    {
        $this->assertTtl($ttlMs);
        $this->client->execute(['SET', $this->keys->idempotency($token->messageId), self::DONE, 'PX', (string) $ttlMs]);
    }

    /**
     * Complessità: O(1), un round trip.
     */
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
