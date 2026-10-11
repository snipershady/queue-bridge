<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\System;

use InvalidArgumentException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use QueueBridge\System\NullAlarm;
use QueueBridge\System\NullSignalHandler;
use QueueBridge\System\PcntlAlarm;
use QueueBridge\System\PcntlSignalHandler;

#[CoversClass(PcntlSignalHandler::class)]
#[CoversClass(NullSignalHandler::class)]
#[CoversClass(PcntlAlarm::class)]
#[CoversClass(NullAlarm::class)]
#[RequiresPhpExtension('pcntl')]
#[RequiresPhpExtension('posix')]
final class SignalAndAlarmTest extends TestCase
{
    #[Override]
    protected function tearDown(): void
    {
        pcntl_alarm(0);
        foreach ([\SIGTERM, \SIGINT, \SIGALRM] as $signal) {
            pcntl_signal($signal, \SIG_DFL);
        }
    }

    public function testPcntlSignalHandlerNotifiesTerminationSignals(): void
    {
        $received = [];
        $pcntlSignalHandler = new PcntlSignalHandler();
        $pcntlSignalHandler->onTerminate(static function (int $signal) use (&$received): void {
            $received[] = $signal;
        });

        posix_kill(posix_getpid(), \SIGTERM);
        posix_kill(posix_getpid(), \SIGINT);

        self::assertSame([\SIGTERM, \SIGINT], $received);
    }

    public function testPcntlSignalHandlerResetRestoresDefaults(): void
    {
        $pcntlSignalHandler = new PcntlSignalHandler();
        $pcntlSignalHandler->onTerminate(static function (int $signal): void {
        });
        $pcntlSignalHandler->reset();

        self::assertSame(\SIG_DFL, pcntl_signal_get_handler(\SIGTERM));
        self::assertSame(\SIG_DFL, pcntl_signal_get_handler(\SIGINT));
    }

    public function testNullSignalHandlerNeverInvokesTheCallback(): void
    {
        $nullSignalHandler = new NullSignalHandler();
        $nullSignalHandler->onTerminate(static function (int $signal): never {
            self::fail('Should not be called');
        });
        $nullSignalHandler->reset();

        self::assertSame(\SIG_DFL, pcntl_signal_get_handler(\SIGTERM));
    }

    public function testPcntlAlarmSchedulesAndFires(): void
    {
        $fired = false;
        $pcntlAlarm = new PcntlAlarm();
        $pcntlAlarm->schedule(5, static function () use (&$fired): void {
            $fired = true;
        });

        self::assertTrue($pcntlAlarm->isSupported());
        self::assertSame(5, pcntl_alarm(5), 'alarm should be pending for 5 seconds');

        posix_kill(posix_getpid(), \SIGALRM);

        self::assertTrue($fired);
    }

    public function testPcntlAlarmCancel(): void
    {
        $pcntlAlarm = new PcntlAlarm();
        $pcntlAlarm->schedule(5, static function (): void {
        });
        $pcntlAlarm->cancel();

        self::assertSame(0, pcntl_alarm(0));
        self::assertSame(\SIG_DFL, pcntl_signal_get_handler(\SIGALRM));
    }

    public function testPcntlAlarmAcceptsOneSecond(): void
    {
        (new PcntlAlarm())->schedule(1, static function (): void {
        });

        self::assertSame(1, pcntl_alarm(0));
    }

    public function testPcntlAlarmRejectsNonPositiveDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new PcntlAlarm())->schedule(0, static function (): void {
        });
    }

    public function testNullAlarmIsUnsupportedAndInert(): void
    {
        $nullAlarm = new NullAlarm();
        $nullAlarm->schedule(1, static function (): never {
            self::fail('Should not be called');
        });
        $nullAlarm->cancel();

        self::assertFalse($nullAlarm->isSupported());
        self::assertSame(0, pcntl_alarm(0));
    }

    public function testNullAlarmRejectsNonPositiveDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new NullAlarm())->schedule(0, static function (): void {
        });
    }
}
