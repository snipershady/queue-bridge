<?php

declare(strict_types=1);

namespace snipershady\Queue\Dsn;

use Closure;
use InvalidArgumentException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use Predis\Client;
use Predis\ClientInterface;
use Redis;
use snipershady\Queue\Factory\StrategyDtoInterface;
use snipershady\Queue\Transport\InMemory\StrategyDtoInMemory;
use snipershady\Queue\Transport\RabbitMq\Client\PhpAmqpLibConnector;
use snipershady\Queue\Transport\RabbitMq\RetryTiers;
use snipershady\Queue\Transport\RabbitMq\StrategyDtoRabbitMq;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Client\PredisClient;
use snipershady\Queue\Transport\Redis\StrategyDtoRedis;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;

/**
 * DSN → DTO di strategia, per CLI e configurazione testuale. Le connessioni sono lazy: si aprono al primo
 * comando, e dopo una caduta vengono ricreate.
 *
 * Opzioni Redis: `client` (phpredis | predis; default phpredis se ext-redis è caricata), `version`
 * (6.0 … 8.4), `verify_version` (bool), `consumer`, `timeout` (s). Opzioni AMQP: `prefetch`, `confirms`
 * (bool), `heartbeat` (s), `timeout` (s), `tiers` (ms separati da virgola).
 *
 * @api
 */
final readonly class StrategyDtoFactory
{
    /** @var Closure(): Redis */
    private Closure $newPhpRedis;

    /** @var Closure(array<string, mixed>, array<string, mixed>): ClientInterface */
    private Closure $newPredis;

    /** @var Closure(AMQPConnectionConfig): AbstractConnection */
    private Closure $newAmqp;

    private bool $phpRedisAvailable;

    /**
     * Le closure creano i client nativi; vanno sostituite solo nei test.
     *
     * @param ?Closure(): Redis                                                     $newPhpRedis
     * @param ?Closure(array<string, mixed>, array<string, mixed>): ClientInterface $newPredis
     * @param ?Closure(AMQPConnectionConfig): AbstractConnection                    $newAmqp
     */
    public function __construct(
        ?Closure $newPhpRedis = null,
        ?Closure $newPredis = null,
        ?Closure $newAmqp = null,
        ?bool $phpRedisAvailable = null,
    ) {
        $this->newPhpRedis = $newPhpRedis ?? static fn (): Redis => new Redis();
        $this->newPredis = $newPredis ?? static fn (array $parameters, array $options): ClientInterface => new Client($parameters, $options);
        $this->newAmqp = $newAmqp ?? AMQPConnectionFactory::create(...);
        $this->phpRedisAvailable = $phpRedisAvailable ?? \extension_loaded('redis');
    }

    /**
     * @throws InvalidArgumentException se il DSN o un'opzione non sono validi
     */
    public function fromDsn(string $dsn): StrategyDtoInterface
    {
        $parsed = DsnParser::parse($dsn);

        return match ($parsed->strategy) {
            StrategyEnum::InMemory => new StrategyDtoInMemory(),
            StrategyEnum::Redis => $this->redis($parsed),
            // default al posto di StrategyEnum::RabbitMq: Xdebug conta come branch scoperto l'UnhandledMatchError
            // implicito dopo l'ultimo arm di un match esaustivo.
            default => $this->rabbitMq($parsed),
        };
    }

    private function redis(Dsn $dsn): StrategyDtoRedis
    {
        $version = '' === $dsn->option('version', '') ? null : (RedisVersionEnum::tryFrom($dsn->option('version', ''))
            ?? throw new InvalidArgumentException(\sprintf('DSN option "version" must be one of %s.', \implode(', ', array_map(static fn (RedisVersionEnum $redisVersionEnum): string => $redisVersionEnum->value, RedisVersionEnum::cases())))));
        if (1 !== \preg_match('/^\d{0,4}$/D', $dsn->path)) {
            throw new InvalidArgumentException(\sprintf('Invalid Redis database "%s": expected a number.', $dsn->path));
        }

        $database = (int) $dsn->path;

        $timeout = $dsn->floatOption('timeout', 2.0);
        $consumer = $dsn->option('consumer', '');
        $client = match ($dsn->option('client', $this->phpRedisAvailable ? 'phpredis' : 'predis')) {
            'phpredis' => new PhpRedisClient(fn (): Redis => $this->connectPhpRedis($dsn, $database, $timeout)),
            'predis' => new PredisClient(($this->newPredis)([
                'scheme' => $dsn->secure ? 'tls' : 'tcp',
                'host' => $dsn->host,
                'port' => $dsn->port ?? 6379,
                'database' => $database,
                'username' => $dsn->user,
                'password' => $dsn->password,
                'timeout' => $timeout,
            ], ['exceptions' => false])),
            default => throw new InvalidArgumentException('DSN option "client" must be "phpredis" or "predis".'),
        };

        return new StrategyDtoRedis(
            $client,
            $version,
            $dsn->boolOption('verify_version', default: true),
            '' === $consumer ? null : $consumer,
        );
    }

    private function connectPhpRedis(Dsn $dsn, int $database, float $timeout): Redis
    {
        $redis = ($this->newPhpRedis)();
        $redis->connect(($dsn->secure ? 'tls://' : '').$dsn->host, $dsn->port ?? 6379, $timeout);

        if (null !== $dsn->password) {
            $redis->auth(null === $dsn->user ? $dsn->password : [$dsn->user, $dsn->password]);
        }

        if (0 !== $database) {
            $redis->select($database);
        }

        return $redis;
    }

    private function rabbitMq(Dsn $dsn): StrategyDtoRabbitMq
    {
        $amqpConnectionConfig = new AMQPConnectionConfig();
        $amqpConnectionConfig->setHost($dsn->host);
        $amqpConnectionConfig->setPort($dsn->port ?? ($dsn->secure ? 5671 : 5672));
        $amqpConnectionConfig->setUser($dsn->user ?? 'guest');
        $amqpConnectionConfig->setPassword($dsn->password ?? 'guest');
        $amqpConnectionConfig->setVhost('' === $dsn->path ? '/' : $dsn->path);
        $amqpConnectionConfig->setIsSecure($dsn->secure);
        $amqpConnectionConfig->setHeartbeat($dsn->intOption('heartbeat', 0));
        $amqpConnectionConfig->setConnectionTimeout($dsn->floatOption('timeout', 3.0));

        $tiers = $dsn->option('tiers', '');

        return new StrategyDtoRabbitMq(
            new PhpAmqpLibConnector(fn (): AbstractConnection => ($this->newAmqp)($amqpConnectionConfig), $dsn->boolOption('confirms', default: true)),
            retryTiers: '' === $tiers ? new RetryTiers() : new RetryTiers($this->tiers($tiers)),
            prefetch: $dsn->intOption('prefetch', 10, 1),
        );
    }

    /**
     * @return list<int>
     */
    private function tiers(string $tiers): array
    {
        $milliseconds = [];
        foreach (explode(',', $tiers) as $tier) {
            if (1 !== \preg_match('/^\d+$/D', \trim($tier))) {
                throw new InvalidArgumentException(\sprintf('DSN option "tiers" must list milliseconds, got "%s".', $tier));
            }

            $milliseconds[] = (int) \trim($tier);
        }

        return $milliseconds;
    }
}
