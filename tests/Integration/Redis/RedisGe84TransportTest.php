<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use QueueBridge\Transport\Redis\Client\PhpRedisClient;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\RedisTransport;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;

#[Group('redis')]
#[CoversClass(RedisTransport::class)]
#[UsesClass(PhpRedisClient::class)]
#[UsesClass(FeatureSelector::class)]
final class RedisGe84TransportTest extends RedisTransportTestCase
{
    #[Override]
    protected function level(): RedisVersionEnum
    {
        return RedisVersionEnum::GE84;
    }
}
