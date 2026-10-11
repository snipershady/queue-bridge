<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Middleware;

use DateTimeImmutable;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Idempotency\AcquireResult;
use snipershady\Queue\Idempotency\AcquireStatus;
use snipershady\Queue\Idempotency\InMemoryIdempotencyStore;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Middleware\IdempotencyMiddleware;
use snipershady\Queue\Middleware\LoggingMiddleware;
use snipershady\Queue\Middleware\TimeoutMiddleware;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Tests\Support\FakeAlarm;
use snipershady\Queue\Tests\Support\FakeClock;
use snipershady\Queue\Tests\Support\RecordingLogger;

#[CoversClass(IdempotencyMiddleware::class)]
#[CoversClass(TimeoutMiddleware::class)]
#[CoversClass(LoggingMiddleware::class)]
#[UsesClass(InMemoryIdempotencyStore::class)]
#[UsesClass(LockToken::class)]
#[UsesClass(AcquireResult::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(NativeRandom::class)]
#[UsesClass(RetryableException::class)]
#[UsesClass(SystemClock::class)]
final class WorkerMiddlewaresTest extends TestCase
{
    public function testIdempotencyRunsTheHandlerOncePerMessageId(): void
    {
        $idempotencyMiddleware = new IdempotencyMiddleware(new InMemoryIdempotencyStore(new FakeClock()));
        $message = $this->message();
        $runs = 0;
        $handler = static function () use (&$runs): void {
            ++$runs;
        };

        $idempotencyMiddleware->process($message, $handler);
        $idempotencyMiddleware->process($message, $handler);
        $idempotencyMiddleware->process($message->withIncrementedAttempts(), $handler);

        self::assertSame(1, $runs);
    }

    public function testIdempotencyRetriesAMessageInProgressElsewhereInsteadOfAcknowledgingIt(): void
    {
        $fakeClock = new FakeClock();
        $inMemoryIdempotencyStore = new InMemoryIdempotencyStore($fakeClock);
        $idempotencyMiddleware = new IdempotencyMiddleware($inMemoryIdempotencyStore, lockTtlMs: 5000);
        $message = $this->message();
        // Un altro worker ha preso il lock ed è morto (o è ancora in esecuzione).
        self::assertSame(AcquireStatus::Acquired, $inMemoryIdempotencyStore->acquire($message->id, 5000)->status);
        $fakeClock->advanceMs(1200);
        $runs = 0;

        try {
            $idempotencyMiddleware->process($message, static function () use (&$runs): void {
                ++$runs;
            });
            self::fail('Expected RetryableException');
        } catch (RetryableException $retryableException) {
            self::assertSame(3800, $retryableException->delayMs);
            self::assertSame(\sprintf('Message %s is being processed by another worker: retrying when its lock expires.', $message->id->value), $retryableException->getMessage());
        }

        self::assertSame(0, $runs);

        // Allo scadere del lock il messaggio ritentato viene elaborato.
        $fakeClock->advanceMs(3800);
        $idempotencyMiddleware->process($message->withIncrementedAttempts(), static function () use (&$runs): void {
            ++$runs;
        });
        self::assertSame(1, $runs);
    }

    public function testIdempotencyReleasesTheLockWhenTheHandlerFails(): void
    {
        $idempotencyMiddleware = new IdempotencyMiddleware(new InMemoryIdempotencyStore(new FakeClock()));
        $message = $this->message();

        try {
            $idempotencyMiddleware->process($message, static function (): never {
                throw new RuntimeException('boom');
            });
            self::fail('Expected RuntimeException');
        } catch (RuntimeException $runtimeException) {
            self::assertSame('boom', $runtimeException->getMessage());
        }

        $runs = 0;
        $idempotencyMiddleware->process($message, static function () use (&$runs): void {
            ++$runs;
        });
        self::assertSame(1, $runs);
    }

    public function testIdempotencyUsesTheConfiguredTtls(): void
    {
        $fakeClock = new FakeClock();
        $inMemoryIdempotencyStore = new InMemoryIdempotencyStore($fakeClock);
        $idempotencyMiddleware = new IdempotencyMiddleware($inMemoryIdempotencyStore, lockTtlMs: 100, doneTtlMs: 200);
        $message = $this->message();

        $idempotencyMiddleware->process($message, static function () use ($inMemoryIdempotencyStore, $message, $fakeClock): void {
            $fakeClock->advanceMs(100);
            // Il lock è scaduto durante l'esecuzione: un altro worker potrebbe acquisirlo.
            self::assertInstanceOf(LockToken::class, $inMemoryIdempotencyStore->acquire($message->id, 1)->token);
        });

        $fakeClock->advanceMs(199);
        self::assertSame(AcquireStatus::Completed, $inMemoryIdempotencyStore->acquire($message->id, 1)->status);
        $fakeClock->advanceMs(1);
        self::assertSame(AcquireStatus::Acquired, $inMemoryIdempotencyStore->acquire($message->id, 1)->status);
    }

    public function testIdempotencyDefaultTtls(): void
    {
        $fakeClock = new FakeClock();
        $inMemoryIdempotencyStore = new InMemoryIdempotencyStore($fakeClock);
        $idempotencyMiddleware = new IdempotencyMiddleware($inMemoryIdempotencyStore);
        $message = $this->message();

        $idempotencyMiddleware->process($message, static function () use ($inMemoryIdempotencyStore, $message, $fakeClock): void {
            $fakeClock->advanceMs(299_999);
            self::assertSame(1, $inMemoryIdempotencyStore->acquire($message->id, 1)->lockRemainingMs);
        });

        $fakeClock->advanceMs(86_399_999);
        self::assertSame(AcquireStatus::Completed, $inMemoryIdempotencyStore->acquire($message->id, 1)->status);
        $fakeClock->advanceMs(1);
        self::assertSame(AcquireStatus::Acquired, $inMemoryIdempotencyStore->acquire($message->id, 1)->status);
    }

    public function testIdempotencyAcceptsMinimalTtls(): void
    {
        $runs = 0;

        (new IdempotencyMiddleware(new InMemoryIdempotencyStore(new FakeClock()), 1, 1))->process($this->message(), static function () use (&$runs): void {
            ++$runs;
        });

        self::assertSame(1, $runs);
    }

    public function testIdempotencyRejectsNonPositiveTtls(): void
    {
        $inMemoryIdempotencyStore = new InMemoryIdempotencyStore(new FakeClock());

        foreach ([[0, 1], [1, 0]] as [$lock, $done]) {
            try {
                new IdempotencyMiddleware($inMemoryIdempotencyStore, $lock, $done);
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertStringContainsString('TTLs must be >= 1 ms', $invalidArgumentException->getMessage());
            }
        }
    }

    public function testTimeoutSchedulesTheAlarmAndCancelsItAfterTheHandler(): void
    {
        $fakeAlarm = new FakeAlarm();
        $timeoutMiddleware = new TimeoutMiddleware($fakeAlarm, 30);
        $scheduledDuringHandler = null;

        $timeoutMiddleware->process($this->message(), static function () use ($fakeAlarm, &$scheduledDuringHandler): void {
            $scheduledDuringHandler = $fakeAlarm->scheduledSeconds;
        });

        self::assertSame(30, $scheduledDuringHandler);
        self::assertSame(1, $fakeAlarm->cancellations);
        self::assertSame(30, $timeoutMiddleware->timeoutSeconds());
    }

    public function testTimeoutInterruptsTheHandlerWithARetryableException(): void
    {
        $fakeAlarm = new FakeAlarm();

        try {
            (new TimeoutMiddleware($fakeAlarm, 5))->process($this->message(), static function () use ($fakeAlarm): void {
                $fakeAlarm->fire();
            });
            self::fail('Expected RetryableException');
        } catch (RetryableException $retryableException) {
            self::assertSame('Handler timed out after 5 seconds.', $retryableException->getMessage());
            self::assertNull($retryableException->delayMs);
        }

        self::assertSame(1, $fakeAlarm->cancellations);
    }

    public function testTimeoutWithoutAlarmSupportWarnsOnceAndRunsTheHandler(): void
    {
        $fakeAlarm = new FakeAlarm(supported: false);
        $recordingLogger = new RecordingLogger();
        $timeoutMiddleware = new TimeoutMiddleware($fakeAlarm, 5, $recordingLogger);
        $runs = 0;

        $timeoutMiddleware->process($this->message(), static function () use (&$runs): void {
            ++$runs;
        });
        $timeoutMiddleware->process($this->message(), static function () use (&$runs): void {
            ++$runs;
        });

        self::assertSame(2, $runs);
        self::assertNull($fakeAlarm->scheduledSeconds);
        self::assertSame(['warning'], $recordingLogger->levels());
        self::assertSame(['seconds' => 5], $recordingLogger->records[0]['context']);
    }

    public function testTimeoutWithoutAlarmSupportAndDefaultLoggerStillRunsTheHandler(): void
    {
        $runs = 0;

        (new TimeoutMiddleware(new FakeAlarm(supported: false), 1))->process($this->message(), static function () use (&$runs): void {
            ++$runs;
        });

        self::assertSame(1, $runs);
    }

    public function testTimeoutRejectsNonPositiveSeconds(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new TimeoutMiddleware(new FakeAlarm(), 0);
    }

    public function testLoggingRecordsStartAndSuccessWithoutTheText(): void
    {
        $fakeClock = new FakeClock();
        $recordingLogger = new RecordingLogger();
        $message = $this->message('secret payload');

        (new LoggingMiddleware($recordingLogger, $fakeClock))->process($message, static function () use ($fakeClock): void {
            $fakeClock->advanceMs(42);
        });

        self::assertSame(['info', 'info'], $recordingLogger->levels());
        $expected = ['message_id' => $message->id->value, 'channel' => 'test', 'attempts' => 0, 'text_bytes' => 14];
        self::assertSame($expected, $recordingLogger->records[0]['context']);
        self::assertSame([...$expected, 'duration_ms' => 42], $recordingLogger->records[1]['context']);
        self::assertStringNotContainsString('secret', serialize($recordingLogger->records));
    }

    public function testLoggingRecordsTheFailureAndRethrows(): void
    {
        $fakeClock = new FakeClock();
        $recordingLogger = new RecordingLogger();
        $runtimeException = new RuntimeException('boom');

        try {
            (new LoggingMiddleware($recordingLogger, $fakeClock))->process($this->message(), static function () use ($fakeClock, $runtimeException): never {
                $fakeClock->advanceMs(7);

                throw $runtimeException;
            });
            self::fail('Expected RuntimeException');
        } catch (RuntimeException $caught) {
            self::assertSame($runtimeException, $caught);
        }

        self::assertSame(['info', 'warning'], $recordingLogger->levels());
        $context = $recordingLogger->records[1]['context'];
        self::assertSame(7, $context['duration_ms']);
        self::assertSame('boom', $context['error']);
        self::assertSame(RuntimeException::class, $context['error_class']);
        self::assertSame($runtimeException, $context['exception']);
    }

    public function testLoggingDefaultsToTheSystemClock(): void
    {
        $recordingLogger = new RecordingLogger();

        (new LoggingMiddleware($recordingLogger))->process($this->message(), static function (): void {
        });

        self::assertGreaterThanOrEqual(0, $recordingLogger->records[1]['context']['duration_ms']);
    }

    private function message(string $text = 'x'): Message
    {
        return new Message(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd'), 'test', $text, new DateTimeImmutable('2026-01-01T00:00:00Z'));
    }
}
