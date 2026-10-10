<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Producer;

use DateTimeImmutable;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use snipershady\Queue\Correlation\CorrelationContext;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Middleware\CorrelationContextMiddleware;
use snipershady\Queue\Producer\Middleware\CorrelationIdMiddleware;
use snipershady\Queue\Producer\Middleware\TraceContextMiddleware;
use snipershady\Queue\Producer\Middleware\TraceContextProviderInterface;

#[CoversClass(CorrelationContext::class)]
#[CoversClass(CorrelationIdMiddleware::class)]
#[CoversClass(CorrelationContextMiddleware::class)]
#[CoversClass(TraceContextMiddleware::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
final class ProducerMiddlewaresTest extends TestCase
{
    private const string CAUSE_ID = '0190f2a4-5b6c-7d8e-9f01-000000000001';

    private const string NEW_ID = '0190f2a4-5b6c-7d8e-9f01-000000000002';

    private const string TRACEPARENT = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

    public function testCorrelationContextIsAStack(): void
    {
        $correlationContext = new CorrelationContext();
        $outer = $this->message(self::CAUSE_ID);
        $inner = $this->message(self::NEW_ID);

        self::assertNull($correlationContext->current());
        $correlationContext->enter($outer);
        $correlationContext->enter($inner);
        self::assertSame($inner, $correlationContext->current());
        $correlationContext->leave();
        self::assertSame($outer, $correlationContext->current());
        $correlationContext->leave();
        $correlationContext->leave();
        self::assertNull($correlationContext->current());
    }

    public function testCorrelationIdDefaultsToTheMessageIdOutsideAHandler(): void
    {
        self::assertSame(['correlation_id' => self::NEW_ID], $this->correlate(new CorrelationContext(), $this->message(self::NEW_ID))->headers);
        self::assertSame(['correlation_id' => self::NEW_ID], $this->correlate(correlationContext: null, message: $this->message(self::NEW_ID))->headers);
    }

    public function testCorrelationIdIsInheritedFromTheMessageBeingHandled(): void
    {
        $correlationContext = new CorrelationContext();
        $correlationContext->enter($this->message(self::CAUSE_ID, ['correlation_id' => 'root']));

        self::assertSame(
            ['correlation_id' => 'root', 'causation_id' => self::CAUSE_ID],
            $this->correlate($correlationContext, $this->message(self::NEW_ID))->headers,
        );
    }

    public function testCauseWithoutCorrelationIdBecomesTheCorrelation(): void
    {
        $correlationContext = new CorrelationContext();
        $correlationContext->enter($this->message(self::CAUSE_ID));

        self::assertSame(
            ['correlation_id' => self::CAUSE_ID, 'causation_id' => self::CAUSE_ID],
            $this->correlate($correlationContext, $this->message(self::NEW_ID))->headers,
        );
    }

    public function testExistingCorrelationHeadersAreKept(): void
    {
        $correlationContext = new CorrelationContext();
        $correlationContext->enter($this->message(self::CAUSE_ID, ['correlation_id' => 'root']));

        self::assertSame(
            ['correlation_id' => 'mine', 'causation_id' => 'explicit'],
            $this->correlate($correlationContext, $this->message(self::NEW_ID, ['correlation_id' => 'mine', 'causation_id' => 'explicit']))->headers,
        );
    }

    public function testCorrelationContextMiddlewareSetsTheContextOnlyDuringTheHandler(): void
    {
        $correlationContext = new CorrelationContext();
        $correlationContextMiddleware = new CorrelationContextMiddleware($correlationContext);
        $message = $this->message(self::CAUSE_ID);
        $seen = null;

        $correlationContextMiddleware->process($message, static function () use ($correlationContext, &$seen): void {
            $seen = $correlationContext->current();
        });

        self::assertSame($message, $seen);
        self::assertNull($correlationContext->current());
    }

    public function testCorrelationContextMiddlewareClearsTheContextWhenTheHandlerFails(): void
    {
        $correlationContext = new CorrelationContext();

        try {
            (new CorrelationContextMiddleware($correlationContext))->process($this->message(self::CAUSE_ID), static function (): never {
                throw new RuntimeException('boom');
            });
            self::fail('Expected RuntimeException');
        } catch (RuntimeException $runtimeException) {
            self::assertSame('boom', $runtimeException->getMessage());
        }

        self::assertNull($correlationContext->current());
    }

    public function testTraceContextIsAdded(): void
    {
        self::assertSame(['traceparent' => self::TRACEPARENT], $this->trace(self::TRACEPARENT, $this->message(self::NEW_ID))->headers);
    }

    /**
     * @return iterable<string, array{?string}>
     */
    public static function ignoredTraceparents(): iterable
    {
        yield 'no active span' => [null];
        yield 'garbage' => ['not-a-traceparent'];
        yield 'unsupported version' => ['01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'];
        yield 'all-zero trace id' => ['00-00000000000000000000000000000000-00f067aa0ba902b7-01'];
        yield 'all-zero span id' => ['00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01'];
        yield 'uppercase' => ['00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01'];
        yield 'trailing newline' => [self::TRACEPARENT."\n"];
    }

    #[DataProvider('ignoredTraceparents')]
    public function testInvalidOrMissingTraceContextIsIgnored(?string $traceparent): void
    {
        self::assertSame([], $this->trace($traceparent, $this->message(self::NEW_ID))->headers);
    }

    public function testTraceContextKeepsExistingHeaders(): void
    {
        self::assertSame(
            ['correlation_id' => 'c-1', 'traceparent' => self::TRACEPARENT],
            $this->trace(self::TRACEPARENT, $this->message(self::NEW_ID, ['correlation_id' => 'c-1']))->headers,
        );
    }

    public function testExistingTraceparentIsKept(): void
    {
        $message = $this->message(self::NEW_ID, ['traceparent' => 'upstream']);

        self::assertSame(['traceparent' => 'upstream'], $this->trace(self::TRACEPARENT, $message)->headers);
    }

    private function correlate(?CorrelationContext $correlationContext, Message $message): Message
    {
        $correlationIdMiddleware = $correlationContext instanceof CorrelationContext
            ? new CorrelationIdMiddleware($correlationContext)
            : new CorrelationIdMiddleware();

        $result = null;
        $correlationIdMiddleware->process($message, static function (Message $message) use (&$result): void {
            $result = $message;
        });
        self::assertInstanceOf(Message::class, $result);

        return $result;
    }

    private function trace(?string $traceparent, Message $message): Message
    {
        $provider = new readonly class($traceparent) implements TraceContextProviderInterface {
            public function __construct(private ?string $traceparent)
            {
            }

            #[Override]
            public function currentTraceparent(): ?string
            {
                return $this->traceparent;
            }
        };

        $result = null;
        (new TraceContextMiddleware($provider))->process($message, static function (Message $message) use (&$result): void {
            $result = $message;
        });
        self::assertInstanceOf(Message::class, $result);

        return $result;
    }

    /**
     * @param array<string, string> $headers
     */
    private function message(string $id, array $headers = []): Message
    {
        return new Message(MessageId::fromString($id), 'test', 'x', new DateTimeImmutable('2026-01-01T00:00:00Z'), headers: $headers);
    }
}
