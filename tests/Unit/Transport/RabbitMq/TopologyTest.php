<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Transport\RabbitMq;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Tests\Support\FakeAmqpChannel;
use snipershady\Queue\Transport\RabbitMq\Client\AmqpMessage;
use snipershady\Queue\Transport\RabbitMq\RabbitMqDelayModeEnum;
use snipershady\Queue\Transport\RabbitMq\RetryTiers;
use snipershady\Queue\Transport\RabbitMq\Topology;

#[CoversClass(Topology::class)]
#[CoversClass(RetryTiers::class)]
#[CoversClass(RabbitMqDelayModeEnum::class)]
#[CoversClass(AmqpMessage::class)]
#[UsesClass(QueueConfig::class)]
final class TopologyTest extends TestCase
{
    public function testNames(): void
    {
        $topology = new Topology(new QueueConfig(name: 'messages', prefix: 'rq'), new RetryTiers([1000, 60_000]));

        self::assertSame('rq.messages', $topology->exchange);
        self::assertSame('rq.messages', $topology->queue);
        self::assertSame('rq.messages.dlq', $topology->deadLetterQueue);
        self::assertSame('rq.messages.retry.1000', $topology->retryQueue(1000));
        self::assertSame(['rq.messages.retry.1000', 'rq.messages.retry.60000'], $topology->retryQueues());
    }

    public function testDeclaresQuorumQueuesWithSafeDeadLettering(): void
    {
        $fakeAmqpChannel = new FakeAmqpChannel();

        (new Topology(new QueueConfig(name: 'messages', maxAttempts: 2, maxDeliveries: 7), new RetryTiers([1000])))->declare($fakeAmqpChannel);

        self::assertSame([['rq.messages', 'topic']], $fakeAmqpChannel->declaredExchanges);
        self::assertSame([['rq.messages', 'rq.messages', '#']], $fakeAmqpChannel->bindings);
        self::assertSame([
            ['rq.messages', [
                'x-queue-type' => 'quorum',
                'x-delivery-limit' => 7,
                'x-dead-letter-exchange' => '',
                'x-dead-letter-routing-key' => 'rq.messages.dlq',
                'x-dead-letter-strategy' => 'at-least-once',
                'x-overflow' => 'reject-publish',
            ], false],
            ['rq.messages.dlq', ['x-queue-type' => 'classic'], false],
            ['rq.messages.retry.1000', [
                'x-queue-type' => 'quorum',
                'x-message-ttl' => 1000,
                'x-dead-letter-exchange' => '',
                'x-dead-letter-routing-key' => 'rq.messages',
                'x-dead-letter-strategy' => 'at-least-once',
                'x-overflow' => 'reject-publish',
            ], false],
        ], $fakeAmqpChannel->declaredQueues);
    }

    public function testDefaultTiers(): void
    {
        self::assertSame([1000, 5000, 30_000, 120_000, 600_000, 3_600_000], (new RetryTiers())->tiersMs);
        self::assertSame(['retry_tiers'], array_map(static fn (RabbitMqDelayModeEnum $rabbitMqDelayModeEnum): string => $rabbitMqDelayModeEnum->value, RabbitMqDelayModeEnum::cases()));
    }

    /**
     * @return iterable<string, array{int, int}>
     */
    public static function tierChoices(): iterable
    {
        yield 'below the smallest tier rounds up' => [1, 1000];
        yield 'exactly a tier' => [5000, 5000];
        yield 'between tiers uses the lower one' => [29_999, 5000];
        yield 'above the largest tier uses the largest' => [10_000_000, 3_600_000];
    }

    #[DataProvider('tierChoices')]
    public function testTierFor(int $remainingMs, int $expected): void
    {
        self::assertSame($expected, (new RetryTiers())->tierFor($remainingMs));
    }

    /**
     * @return iterable<string, array{list<int>}>
     */
    public static function invalidTiers(): iterable
    {
        yield 'empty' => [[]];
        yield 'zero' => [[0, 1000]];
        yield 'not increasing' => [[1000, 1000]];
        yield 'decreasing' => [[5000, 1000]];
    }

    /**
     * @param list<int> $tiers
     */
    #[DataProvider('invalidTiers')]
    public function testRejectsInvalidTiers(array $tiers): void
    {
        $this->expectException(InvalidArgumentException::class);

        new RetryTiers($tiers);
    }

    public function testAmqpMessageDefaults(): void
    {
        $amqpMessage = new AmqpMessage('body');

        self::assertSame(['body', [], null, null], [$amqpMessage->body, $amqpMessage->headers, $amqpMessage->messageId, $amqpMessage->deliveryTag]);
    }
}
