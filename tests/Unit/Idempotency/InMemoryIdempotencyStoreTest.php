<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Idempotency;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Idempotency\AcquireResult;
use QueueBridge\Idempotency\AcquireStatus;
use QueueBridge\Idempotency\InMemoryIdempotencyStore;
use QueueBridge\Idempotency\LockToken;
use QueueBridge\Message\MessageId;
use QueueBridge\System\NativeRandom;
use QueueBridge\Tests\Support\FakeClock;

#[CoversClass(InMemoryIdempotencyStore::class)]
#[CoversClass(LockToken::class)]
#[CoversClass(AcquireResult::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(NativeRandom::class)]
final class InMemoryIdempotencyStoreTest extends TestCase
{
    private FakeClock $fakeClock;

    private InMemoryIdempotencyStore $inMemoryIdempotencyStore;

    private MessageId $messageId;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock();
        $this->inMemoryIdempotencyStore = new InMemoryIdempotencyStore($this->fakeClock);
        $this->messageId = MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-23456789abcd');
    }

    public function testLockIsExclusiveUntilItExpires(): void
    {
        $acquireResult = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);

        self::assertSame(AcquireStatus::Acquired, $acquireResult->status);
        self::assertSame(0, $acquireResult->lockRemainingMs);
        $lockToken = $acquireResult->token;
        self::assertInstanceOf(LockToken::class, $lockToken);
        self::assertSame($this->messageId, $lockToken->messageId);
        self::assertSame(32, \strlen($lockToken->token));
        $this->assertInProgress(1000);

        $this->fakeClock->advanceMs(999);
        $this->assertInProgress(1);

        $this->fakeClock->advanceMs(1);
        $second = $this->acquire();
        self::assertNotSame($lockToken->token, $second->token);
    }

    public function testCompletedMessageIsRememberedForTheDoneTtl(): void
    {
        $lockToken = $this->acquire();
        $this->inMemoryIdempotencyStore->complete($lockToken, 5000);

        $this->fakeClock->advanceMs(4999);
        $this->assertCompleted();

        $this->fakeClock->advanceMs(1);
        $this->acquire();
    }

    public function testReleaseFreesTheLockOnlyForItsOwner(): void
    {
        $lockToken = $this->acquire();
        $this->inMemoryIdempotencyStore->release(new LockToken($this->messageId, 'someone-else'));
        $this->assertInProgress(1000);

        $this->inMemoryIdempotencyStore->release($lockToken);
        $this->acquire();
    }

    public function testReleaseDoesNotForgetACompletedMessage(): void
    {
        $lockToken = $this->acquire();
        $this->inMemoryIdempotencyStore->complete($lockToken, 5000);
        $this->inMemoryIdempotencyStore->release($lockToken);

        $this->assertCompleted();
    }

    public function testReleaseOfUnknownLockIsANoOp(): void
    {
        $this->inMemoryIdempotencyStore->release(new LockToken($this->messageId, 'nobody'));

        // Anche il TTL minimo (1 ms) è accettato.
        self::assertSame(AcquireStatus::Acquired, $this->inMemoryIdempotencyStore->acquire($this->messageId, 1)->status);
    }

    public function testAcquireRejectsNonPositiveTtl(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->inMemoryIdempotencyStore->acquire($this->messageId, 0);
    }

    public function testCompleteRejectsNonPositiveTtl(): void
    {
        $lockToken = $this->acquire();

        $this->expectException(InvalidArgumentException::class);

        $this->inMemoryIdempotencyStore->complete($lockToken, 0);
    }

    public function testInProgressRequiresAPositiveRemainingTime(): void
    {
        self::assertSame(1, AcquireResult::inProgress(1)->lockRemainingMs);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Remaining lock time must be >= 1 ms, got 0.');

        AcquireResult::inProgress(0);
    }

    public function testLockTokenMustNotBeEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new LockToken($this->messageId, '');
    }

    private function acquire(): LockToken
    {
        $acquireResult = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);
        self::assertSame(AcquireStatus::Acquired, $acquireResult->status);
        self::assertInstanceOf(LockToken::class, $acquireResult->token);

        return $acquireResult->token;
    }

    private function assertInProgress(int $lockRemainingMs): void
    {
        $acquireResult = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);

        self::assertSame(AcquireStatus::InProgress, $acquireResult->status);
        self::assertNull($acquireResult->token);
        self::assertSame($lockRemainingMs, $acquireResult->lockRemainingMs);
    }

    private function assertCompleted(): void
    {
        $acquireResult = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);

        self::assertSame(AcquireStatus::Completed, $acquireResult->status);
        self::assertNull($acquireResult->token);
        self::assertSame(0, $acquireResult->lockRemainingMs);
    }
}
