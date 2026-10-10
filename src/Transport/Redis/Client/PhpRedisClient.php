<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Client;

use Closure;
use Override;
use Redis;
use RedisException;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;

/**
 * Adapter per `ext-redis`. Con una factory, dopo un errore di connessione ricrea il client e ritenta il
 * comando una volta (un comando ritentato può essere eseguito due volte: coerente con at-least-once).
 *
 * @api
 */
final class PhpRedisClient implements RedisClientInterface
{
    private ?Redis $redis;

    /** @var ?Closure(): Redis */
    private readonly ?Closure $factory;

    /**
     * @param Redis|Closure(): Redis $redis istanza già connessa, o factory che ne crea una
     */
    public function __construct(Redis|Closure $redis)
    {
        $this->redis = $redis instanceof Redis ? $redis : null;
        $this->factory = $redis instanceof Closure ? $redis : null;
    }

    #[Override]
    public function execute(array $command): array|int|string|null
    {
        return $this->withReconnect(function (Redis $redis) use ($command): array|int|string|null {
            $redis->clearLastError();
            $reply = $redis->rawcommand(...$command);
            $error = $redis->getLastError();

            if (false === $reply && null !== $error) {
                $redis->clearLastError();

                throw CommandFailedException::rejected($command[0], $error);
            }

            return $this->normalize($reply);
        });
    }

    #[Override]
    public function pipeline(array $commands): array
    {
        return $this->withReconnect(function (Redis $redis) use ($commands): array {
            $redis->clearLastError();
            $pipeline = $redis->multi(Redis::PIPELINE);
            if ($pipeline instanceof Redis) {
                foreach ($commands as $command) {
                    $pipeline->rawcommand(...$command);
                }

                $replies = $pipeline->exec();
            } else {
                $replies = false;
            }

            $error = $redis->getLastError() ?? 'unknown error';
            $redis->clearLastError();

            $results = [];
            foreach ($commands as $index => $command) {
                $reply = \is_array($replies) ? ($replies[$index] ?? false) : false;
                $normalized = $this->normalize($reply);
                $results[] = $normalized ?? CommandFailedException::rejected($command[0], $error);
            }

            return $results;
        });
    }

    /**
     * @template T
     *
     * @param Closure(Redis): T $operation
     *
     * @throws ConnectionException
     *
     * @return T
     */
    private function withReconnect(Closure $operation): mixed
    {
        try {
            return $operation($this->connection());
        } catch (RedisException $redisException) {
            if (!$this->factory instanceof Closure) {
                throw ConnectionException::unreachable('Redis', $redisException->getMessage(), $redisException);
            }

            $this->redis = null;

            try {
                return $operation($this->connection());
            } catch (RedisException $retryException) {
                throw ConnectionException::unreachable('Redis', $retryException->getMessage(), $retryException);
            }
        }
    }

    private function connection(): Redis
    {
        if (!$this->redis instanceof Redis) {
            \assert($this->factory instanceof Closure);
            $this->redis = ($this->factory)();
        }

        return $this->redis;
    }

    /**
     * ext-redis restituisce false per nil e true per gli status: si riportano a null e "OK".
     *
     * @return array<mixed>|int|string|null
     */
    private function normalize(mixed $reply): array|int|string|null
    {
        return match (true) {
            false === $reply, null === $reply => null,
            true === $reply => 'OK',
            \is_array($reply) => array_map($this->normalize(...), $reply),
            \is_int($reply), \is_string($reply) => $reply,
            \is_float($reply) => (string) $reply,
            default => throw CommandFailedException::unexpectedReply('ext-redis', 'a RESP2 value'),
        };
    }
}
