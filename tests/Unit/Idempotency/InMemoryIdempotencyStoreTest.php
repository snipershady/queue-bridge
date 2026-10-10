<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Idempotency;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Idempotency\InMemoryIdempotencyStore;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\MessageId;
use snipershady\Queue\System\NativeRandom;
use snipershady\Queue\Tests\Support\FakeClock;

#[CoversClass(InMemoryIdempotencyStore::class)]
#[CoversClass(LockToken::class)]
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
        $lockToken = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);

        self::assertInstanceOf(LockToken::class, $lockToken);
        self::assertSame($this->messageId, $lockToken->messageId);
        self::assertSame(32, \strlen($lockToken->token));
        self::assertNull($this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));

        $this->fakeClock->advanceMs(999);
        self::assertNull($this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));

        $this->fakeClock->advanceMs(1);
        $second = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);
        self::assertInstanceOf(LockToken::class, $second);
        self::assertNotSame($lockToken->token, $second->token);
    }

    public function testCompletedMessageIsRememberedForTheDoneTtl(): void
    {
        $lockToken = $this->acquire();
        $this->inMemoryIdempotencyStore->complete($lockToken, 5000);

        $this->fakeClock->advanceMs(4999);
        self::assertNull($this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));

        $this->fakeClock->advanceMs(1);
        self::assertInstanceOf(LockToken::class, $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));
    }

    public function testReleaseFreesTheLockOnlyForItsOwner(): void
    {
        $lockToken = $this->acquire();
        $this->inMemoryIdempotencyStore->release(new LockToken($this->messageId, 'someone-else'));
        self::assertNull($this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));

        $this->inMemoryIdempotencyStore->release($lockToken);
        self::assertInstanceOf(LockToken::class, $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));
    }

    public function testReleaseDoesNotForgetACompletedMessage(): void
    {
        $lockToken = $this->acquire();
        $this->inMemoryIdempotencyStore->complete($lockToken, 5000);
        $this->inMemoryIdempotencyStore->release($lockToken);

        self::assertNull($this->inMemoryIdempotencyStore->acquire($this->messageId, 1000));
    }

    public function testReleaseOfUnknownLockIsANoOp(): void
    {
        $this->inMemoryIdempotencyStore->release(new LockToken($this->messageId, 'nobody'));

        self::assertInstanceOf(LockToken::class, $this->inMemoryIdempotencyStore->acquire($this->messageId, 1));
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

    public function testLockTokenMustNotBeEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new LockToken($this->messageId, '');
    }

    private function acquire(): LockToken
    {
        $lockToken = $this->inMemoryIdempotencyStore->acquire($this->messageId, 1000);
        self::assertInstanceOf(LockToken::class, $lockToken);

        return $lockToken;
    }
}
