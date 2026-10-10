<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use snipershady\Queue\Transport\Redis\Client\PhpRedisClient;
use snipershady\Queue\Transport\Redis\Feature\FeatureSelector;
use snipershady\Queue\Transport\Redis\RedisTransport;
use snipershady\Queue\Transport\Redis\Version\RedisVersionEnum;

#[Group('redis')]
#[CoversClass(RedisTransport::class)]
#[UsesClass(PhpRedisClient::class)]
#[UsesClass(FeatureSelector::class)]
final class RedisGe70TransportTest extends RedisTransportTestCase
{
    #[Override]
    protected function level(): RedisVersionEnum
    {
        return RedisVersionEnum::GE70;
    }
}
