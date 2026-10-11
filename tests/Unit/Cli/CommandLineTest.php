<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Cli;

use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Cli\CommandLine;

#[CoversClass(CommandLine::class)]
final class CommandLineTest extends TestCase
{
    private mixed $originalArgv = null;

    private bool $hadArgv = false;

    #[Override]
    protected function setUp(): void
    {
        $this->hadArgv = \array_key_exists('argv', $_SERVER);
        $this->originalArgv = $_SERVER['argv'] ?? null;
    }

    #[Override]
    protected function tearDown(): void
    {
        if ($this->hadArgv) {
            $_SERVER['argv'] = $this->originalArgv;
        } else {
            unset($_SERVER['argv']);
        }
    }

    public function testReturnsTheArgumentsAfterTheScriptName(): void
    {
        $_SERVER['argv'] = ['bin/queue-worker', 'work', '--queue=orders'];

        self::assertSame(['work', '--queue=orders'], CommandLine::arguments());
    }

    public function testNestedValuesAreDropped(): void
    {
        $_SERVER['argv'] = ['script', ['nested'], 'stats'];

        self::assertSame(['stats'], CommandLine::arguments());
    }

    public function testMissingArgvGivesAnEmptyList(): void
    {
        unset($_SERVER['argv']);

        self::assertSame([], CommandLine::arguments());
    }

    public function testNonArrayArgvGivesAnEmptyList(): void
    {
        $_SERVER['argv'] = 'not an array';

        self::assertSame([], CommandLine::arguments());
    }
}
