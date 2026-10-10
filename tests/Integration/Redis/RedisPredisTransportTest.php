<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use Predis\Client;
use snipershady\Queue\Transport\Redis\Client\PredisClient;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;
use snipershady\Queue\Transport\Redis\Feature\FeatureSelector;
use snipershady\Queue\Transport\Redis\RedisTransport;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;

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
