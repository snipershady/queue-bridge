<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\System;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Message\MessageId;
use QueueBridge\System\NativeRandom;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\FakeRandom;

#[CoversClass(UuidV7Generator::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(NativeRandom::class)]
final class UuidV7GeneratorTest extends TestCase
{
    private const string NOW = '2026-01-01T00:00:00.123Z';

    private const string NOW_MS_HEX = '019b76daa87b';

    public function testEncodesTimestampVersionAndVariant(): void
    {
        $uuidV7Generator = new UuidV7Generator(new FakeClock(self::NOW), new FakeRandom(0xFF));

        $id = $uuidV7Generator->generate()->value;

        self::assertSame(self::NOW_MS_HEX, \str_replace('-', '', \substr($id, 0, 13)));
        self::assertSame('7', $id[14]);
        self::assertSame('b', $id[19]);
        // 11 bit di seed (0x7FF) nel contatore; 62 bit casuali tutti a 1.
        self::assertSame('019b76da-a87b-77ff-bfff-ffffffffffff', $id);
    }

    public function testUsesBothSeedBytesAndTheFirstTailByteForTheVariant(): void
    {
        $uuidV7Generator = new UuidV7Generator(
            new FakeClock(self::NOW),
            new FakeRandom(0x00, ["\x01\x02", "\x3F\xAA\xBB\xCC\xDD\xEE\xFF\x11"]),
        );

        // contatore = 0x0102 & 0x7FF; variante = 0x80 | (0x3F & 0x3F).
        self::assertSame('019b76da-a87b-7102-bfaa-bbccddeeff11', $uuidV7Generator->generate()->value);
    }

    public function testIdsInTheSameMillisecondAreStrictlyIncreasing(): void
    {
        $uuidV7Generator = new UuidV7Generator(new FakeClock(self::NOW), new FakeRandom(0x00));

        $first = $uuidV7Generator->generate()->value;
        $second = $uuidV7Generator->generate()->value;

        self::assertSame('019b76da-a87b-7000-8000-000000000000', $first);
        self::assertSame('019b76da-a87b-7001-8000-000000000000', $second);
    }

    public function testCounterOverflowAdvancesTheLogicalTimestamp(): void
    {
        $uuidV7Generator = new UuidV7Generator(new FakeClock(self::NOW), new FakeRandom(0xFF));

        $uuidV7Generator->generate(); // contatore = 0x7FF
        for ($i = 0x7FF; $i < 0xFFF; ++$i) {
            $uuidV7Generator->generate();
        }

        $last = $uuidV7Generator->generate()->value;

        self::assertSame('019b76da-a87c-7000-bfff-ffffffffffff', $last);
    }

    public function testClockGoingBackwardsKeepsIdsIncreasing(): void
    {
        $fakeClock = new FakeClock(self::NOW);
        $uuidV7Generator = new UuidV7Generator($fakeClock, new FakeRandom(0x00));

        $first = $uuidV7Generator->generate()->value;
        $fakeClock->advanceMs(-1000);
        $second = $uuidV7Generator->generate()->value;

        self::assertGreaterThan($first, $second);
        self::assertSame(self::NOW_MS_HEX, \str_replace('-', '', \substr($second, 0, 13)));
    }

    public function testNewMillisecondReseedsTheCounter(): void
    {
        $fakeClock = new FakeClock(self::NOW);
        $uuidV7Generator = new UuidV7Generator($fakeClock, new FakeRandom(0x00));

        $uuidV7Generator->generate();
        $uuidV7Generator->generate();

        $fakeClock->advanceMs(1);

        self::assertSame('019b76da-a87c-7000-8000-000000000000', $uuidV7Generator->generate()->value);
    }

    public function testDefaultRandomSourceProducesUniqueIds(): void
    {
        $uuidV7Generator = new UuidV7Generator(new FakeClock(self::NOW));

        $ids = [];
        for ($i = 0; $i < 100; ++$i) {
            $ids[] = $uuidV7Generator->generate()->value;
        }

        self::assertCount(100, array_unique($ids));
        $sorted = $ids;
        sort($sorted);
        self::assertSame($sorted, $ids);
    }
}
