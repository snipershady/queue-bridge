<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use Predis\Client;
use QueueBridge\Transport\Redis\Client\PredisClient;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\RedisTransport;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;

#[Group('redis')]
#[CoversClass(RedisTransport::class)]
#[CoversClass(PredisClient::class)]
#[UsesClass(FeatureSelector::class)]
final class RedisPredisTransportTest extends RedisTransportTestCase
{
    #[Override]
    protected function level(): RedisVersionEnum
    {
        return RedisVersionEnum::GE62;
    }

    #[Override]
    protected function connect(): RedisClientInterface
    {
        [$host, $port, $database] = self::dsn();

        return new PredisClient(new Client(['host' => $host, 'port' => $port, 'database' => $database], ['exceptions' => false]));
    }
}
