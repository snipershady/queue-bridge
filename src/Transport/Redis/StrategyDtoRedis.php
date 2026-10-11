<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis;

use Closure;
use InvalidArgumentException;
use Override;
use Predis\ClientInterface;
use Redis;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Client\PredisClient;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;

/**
 * Backend Redis Streams. Le classi native (ext-redis, Predis) compaiono solo nei named constructor, che si
 * chiamano solo se il client è installato.
 *
 * @api
 */
final readonly class StrategyDtoRedis implements StrategyDtoInterface
{
    /**
     * @param ?RedisVersionEnum $version       livello forzato; null = rilevato da INFO server
     * @param bool              $verifyVersion con un livello forzato, fallisce se supera la versione del server
     * @param ?non-empty-string $consumer      nome del consumer nel gruppo (es. nome del pod); null = generato
     *
     * @throws InvalidArgumentException se $consumer è vuoto
     */
    public function __construct(
        public RedisClientInterface $client,
        public ?RedisVersionEnum $version = null,
        public bool $verifyVersion = true,
        public ?string $consumer = null,
    ) {
        if (null !== $consumer && '' === \trim($consumer)) {
            throw new InvalidArgumentException('Consumer name must not be empty.');
        }
    }

    /**
     * @param Redis|Closure(): Redis $redis istanza connessa, o factory per la riconnessione automatica
     */
    public static function fromPhpRedis(Redis|Closure $redis, ?RedisVersionEnum $version = null): self
    {
        return new self(new PhpRedisClient($redis), $version);
    }

    public static function fromPredis(ClientInterface $predis, ?RedisVersionEnum $version = null): self
    {
        return new self(new PredisClient($predis), $version);
    }

    #[Override]
    public function name(): string
    {
        return 'redis';
    }
}
