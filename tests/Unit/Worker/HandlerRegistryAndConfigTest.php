<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Worker;

use DateTimeImmutable;
use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Retry\ExponentialBackoff;
use snipershady\Queue\Retry\NoRetry;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Worker\CallableHandler;
use snipershady\Queue\Worker\ExpiredMessagePolicy;
use snipershady\Queue\Worker\HandlerInterface;
use snipershady\Queue\Worker\HandlerRegistry;
use snipershady\Queue\Worker\UnknownChannelPolicy;
use snipershady\Queue\Worker\WorkerConfig;
use snipershady\Queue\Worker\WorkerExitReason;

#[CoversClass(HandlerRegistry::class)]
#[CoversClass(CallableHandler::class)]
#[CoversClass(WorkerConfig::class)]
#[CoversClass(WorkerExitReason::class)]
#[CoversClass(UnknownChannelPolicy::class)]
#[CoversClass(ExpiredMessagePolicy::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(DecodedText::class)]
#[UsesClass(ExponentialBackoff::class)]
#[UsesClass(NativeRandom::class)]
final class HandlerRegistryAndConfigTest extends TestCase
{
    public function testResolvesRegisteredHandlerThenFallback(): void
    {
        $handlerRegistry = new HandlerRegistry();
        self::assertNull($handlerRegistry->resolve('orders'));

        $orders = $this->handler();
        $fallback = $this->handler();
        $handlerRegistry->register('orders', $orders);
        $handlerRegistry->register(HandlerRegistry::FALLBACK, $fallback);

        self::assertSame($orders, $handlerRegistry->resolve('orders'));
        self::assertSame($fallback, $handlerRegistry->resolve('invoices'));
    }

    public function testRegisteringAgainReplacesTheHandler(): void
    {
        $handlerRegistry = new HandlerRegistry();
        $second = $this->handler();
        $handlerRegistry->register('orders', $this->handler());
        $handlerRegistry->register('orders', $second);

        self::assertSame($second, $handlerRegistry->resolve('orders'));
    }

    public function testCallablesAreWrapped(): void
    {
        $handlerRegistry = new HandlerRegistry();
        $seen = null;
        $handlerRegistry->register('orders', static function (Message $message, DecodedText $decodedText) use (&$seen): void {
            $seen = [$message->text, $decodedText->raw];
        });

        $handler = $handlerRegistry->resolve('orders');
        self::assertInstanceOf(CallableHandler::class, $handler);

        $handler->handle(
            new Message(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd'), 'orders', 'x', new DateTimeImmutable()),
            DecodedText::plain('x'),
        );
        self::assertSame(['x', 'x'], $seen);
    }

    public function testRejectsInvalidChannels(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new HandlerRegistry())->register('bad channel', $this->handler());
    }

    public function testWorkerConfigDefaults(): void
    {
        $workerConfig = new WorkerConfig();

        self::assertSame(10, $workerConfig->batchSize);
        self::assertSame(1000, $workerConfig->receiveTimeoutMs);
        self::assertNull($workerConfig->maxMessages);
        self::assertNull($workerConfig->maxRuntimeSeconds);
        self::assertNull($workerConfig->memoryLimitMb);
        self::assertFalse($workerConfig->stopWhenEmpty);
        self::assertSame(5000, $workerConfig->maintenanceIntervalMs);
        self::assertSame(1000, $workerConfig->gcEveryMessages);
        self::assertInstanceOf(ExponentialBackoff::class, $workerConfig->retryStrategy);
        self::assertFalse($workerConfig->retryAnyThrowable);
        self::assertSame(UnknownChannelPolicy::DeadLetter, $workerConfig->unknownChannelPolicy);
        self::assertSame(ExpiredMessagePolicy::Discard, $workerConfig->expiredMessagePolicy);
        self::assertSame([], $workerConfig->middlewares);
        self::assertSame([], $workerConfig->listeners);
    }

    public function testWorkerConfigAcceptsBoundaryValues(): void
    {
        $workerConfig = new WorkerConfig(
            batchSize: 1,
            receiveTimeoutMs: 0,
            maxMessages: 1,
            maxRuntimeSeconds: 1,
            memoryLimitMb: 1,
            maintenanceIntervalMs: 0,
            gcEveryMessages: 0,
            retryStrategy: new NoRetry(),
        );

        self::assertSame(1, $workerConfig->batchSize);
        self::assertSame(0, $workerConfig->gcEveryMessages);
    }

    /**
     * @return iterable<string, array{array<string, int>, string}>
     */
    public static function invalidWorkerConfigs(): iterable
    {
        yield 'batchSize' => [['batchSize' => 0], 'batchSize must be >= 1, got 0.'];
        yield 'receiveTimeoutMs' => [['receiveTimeoutMs' => -1], 'receiveTimeoutMs must be >= 0'];
        yield 'maxMessages' => [['maxMessages' => 0], 'maxMessages must be >= 1'];
        yield 'maxRuntimeSeconds' => [['maxRuntimeSeconds' => 0], 'maxRuntimeSeconds must be >= 1'];
        yield 'memoryLimitMb' => [['memoryLimitMb' => 0], 'memoryLimitMb must be >= 1'];
        yield 'maintenanceIntervalMs' => [['maintenanceIntervalMs' => -1], 'maintenanceIntervalMs must be >= 0'];
        yield 'gcEveryMessages' => [['gcEveryMessages' => -1], 'gcEveryMessages must be >= 0'];
    }

    /**
     * @param array<string, int> $arguments
     */
    #[DataProvider('invalidWorkerConfigs')]
    public function testWorkerConfigRejectsInvalidValues(array $arguments, string $error): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($error);

        new WorkerConfig(...$arguments);
    }

    public function testEnumWireValuesAreStable(): void
    {
        self::assertSame(
            ['stop_requested', 'message_limit_reached', 'time_limit_reached', 'memory_limit_reached', 'queue_empty'],
            array_map(static fn (WorkerExitReason $workerExitReason): string => $workerExitReason->value, WorkerExitReason::cases()),
        );
        self::assertSame(['dead_letter', 'discard'], array_map(static fn (UnknownChannelPolicy $unknownChannelPolicy): string => $unknownChannelPolicy->value, UnknownChannelPolicy::cases()));
        self::assertSame(['discard', 'dead_letter'], array_map(static fn (ExpiredMessagePolicy $expiredMessagePolicy): string => $expiredMessagePolicy->value, ExpiredMessagePolicy::cases()));
    }

    private function handler(): HandlerInterface
    {
        return new class implements HandlerInterface {
            #[Override]
            public function handle(Message $message, DecodedText $text): void
            {
            }
        };
    }
}
