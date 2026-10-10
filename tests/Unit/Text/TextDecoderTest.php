<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Unit\Text;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use snipershady\Queue\Text\DecodedText;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Text\TextKind;

#[CoversClass(TextDecoder::class)]
#[CoversClass(DecodedText::class)]
#[CoversClass(TextKind::class)]
final class TextDecoderTest extends TestCase
{
    /**
     * @return iterable<string, array{string, array<mixed>|string|int|float|bool|null}>
     */
    public static function jsonTexts(): iterable
    {
        yield 'object as associative array' => ['{"a":1,"b":{"c":[true,null]}}', ['a' => 1, 'b' => ['c' => [true, null]]]];
        yield 'array' => ['[1,2,3]', [1, 2, 3]];
        yield 'empty object' => ['{}', []];
        yield 'integer' => ['123', 123];
        yield 'float' => ['1.5', 1.5];
        yield 'string' => ['"x"', 'x'];
        yield 'boolean' => ['true', true];
        yield 'null' => ['null', null];
        yield 'big integer kept as string' => ['12345678901234567890', '12345678901234567890'];
        yield 'surrounding whitespace' => [" {\"a\":1}\n", ['a' => 1]];
        yield 'unicode' => ['{"t":"è ✓"}', ['t' => 'è ✓']];
    }

    /**
     * @param array<mixed>|string|int|float|bool|null $expected
     */
    #[DataProvider('jsonTexts')]
    public function testDecodesValidJson(string $text, array|string|int|float|bool|null $expected): void
    {
        $decodedText = (new TextDecoder())->decode($text);

        self::assertTrue($decodedText->isJson());
        self::assertSame(TextKind::Json, $decodedText->kind);
        self::assertSame($expected, $decodedText->value);
        self::assertSame($text, $decodedText->raw);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function plainTexts(): iterable
    {
        yield 'empty' => [''];
        yield 'free text' => ['some json, some data, some text'];
        yield 'broken json' => ['{"a":'];
        yield 'single quotes' => ["{'a':1}"];
        yield 'trailing comma' => ['[1,2,]'];
        yield 'invalid utf-8' => ["\xFF\xFE"];
    }

    #[DataProvider('plainTexts')]
    public function testFallsBackToPlainTextWithoutThrowing(string $text): void
    {
        $decodedText = (new TextDecoder())->decode($text);

        self::assertFalse($decodedText->isJson());
        self::assertSame(TextKind::Plain, $decodedText->kind);
        self::assertSame($text, $decodedText->value);
        self::assertSame($text, $decodedText->raw);
    }

    public function testJsonDeeperThanMaxDepthIsPlainText(): void
    {
        // Semantica di ext-json: un array a n livelli richiede depth >= n + 1.
        $textDecoder = new TextDecoder(2);

        self::assertTrue($textDecoder->decode('[1]')->isJson());
        self::assertFalse($textDecoder->decode('[[1]]')->isJson());
    }

    public function testAcceptsBoundaryDepths(): void
    {
        self::assertTrue((new TextDecoder(1))->decode('1')->isJson());
        self::assertFalse((new TextDecoder(1))->decode('[1]')->isJson());
        self::assertTrue((new TextDecoder(2_147_483_647))->decode('[[1]]')->isJson());
    }

    /**
     * @return iterable<string, array{int}>
     */
    public static function invalidDepths(): iterable
    {
        yield 'zero' => [0];
        yield 'above ext-json limit' => [2_147_483_648];
    }

    #[DataProvider('invalidDepths')]
    public function testRejectsInvalidMaxDepth(int $depth): void
    {
        $this->expectException(InvalidArgumentException::class);

        new TextDecoder($depth);
    }
}
