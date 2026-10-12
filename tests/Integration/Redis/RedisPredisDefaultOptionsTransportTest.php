<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Integration\Redis;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use Predis\Client;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Transport\Redis\Client\PredisClient;
use QueueBridge\Transport\Redis\Client\PredisPipeline;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\RedisTransport;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;

/**
 * Contract test con Predis configurato con le opzioni di default (`exceptions => true`): la pipeline deve comunque
 * riportare l'esito di ogni comando.
 */
#[Group('redis')]
#[CoversClass(RedisTransport::class)]
#[CoversClass(PredisClient::class)]
#[CoversClass(PredisPipeline::class)]
#[UsesClass(FeatureSelector::class)]
final class RedisPredisDefaultOptionsTransportTest extends RedisTransportTestCase
{
    public function testAnErrorInThePipelineDoesNotHideTheCommandsThatSucceeded(): void
    {
        $prefix = 'it'.bin2hex(random_bytes(6));
        $redisClient = $this->connect();

        try {
            $results = $redisClient->pipeline([
                ['SET', $prefix.':string', 'v'],
                ['XADD', $prefix.':string', '*', 'a', '1'],
                ['SET', $prefix.':after', 'v'],
            ]);

            self::assertSame('OK', $results[0]);
            self::assertInstanceOf(CommandFailedException::class, $results[1]);
            self::assertStringContainsString('WRONGTYPE', $results[1]->getMessage());
            self::assertSame('OK', $results[2]);
            self::assertSame('v', $redisClient->execute(['GET', $prefix.':after']), 'the connection is still usable');
        } finally {
            $redisClient->execute(['DEL', $prefix.':string', $prefix.':after']);
        }
    }

    #[Override]
    protected function level(): RedisVersionEnum
    {
        return RedisVersionEnum::GE62;
    }

    #[Override]
    protected function connect(): RedisClientInterface
    {
        [$host, $port, $database] = self::dsn();

        return new PredisClient(new Client(['host' => $host, 'port' => $port, 'database' => $database]));
    }
}
