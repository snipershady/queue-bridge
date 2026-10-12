<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis\Version;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\VersionMismatchException;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Tests\Support\RecordingRedisClient;
use QueueBridge\Transport\Redis\Version\RedisCapabilities;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;
use QueueBridge\Transport\Redis\Version\VersionDetector;

#[CoversClass(RedisVersionEnum::class)]
#[CoversClass(RedisCapabilities::class)]
#[CoversClass(VersionDetector::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(VersionMismatchException::class)]
final class RedisVersionTest extends TestCase
{
    /**
     * @return iterable<string, array{string, RedisVersionEnum}>
     */
    public static function serverVersions(): iterable
    {
        yield '6.0.0' => ['6.0.0', RedisVersionEnum::GE60];
        yield '6.0.20' => ['6.0.20', RedisVersionEnum::GE60];
        yield '6.2.14' => ['6.2.14', RedisVersionEnum::GE62];
        yield '7.0.15' => ['7.0.15', RedisVersionEnum::GE70];
        yield '7.4.2 (Valkey/ElastiCache report 7.2)' => ['7.4.2', RedisVersionEnum::GE70];
        yield '8.0.2' => ['8.0.2', RedisVersionEnum::GE80];
        yield '8.1.0' => ['8.1.0', RedisVersionEnum::GE80];
        yield '8.2.1' => ['8.2.1', RedisVersionEnum::GE82];
        yield '8.4.0' => ['8.4.0', RedisVersionEnum::GE84];
        yield '9.0.0' => ['9.0.0', RedisVersionEnum::GE84];
        yield '10.1' => ['10.1', RedisVersionEnum::GE84];
        yield 'v8.0.0' => ['v8.0.0', RedisVersionEnum::GE80];
    }

    #[DataProvider('serverVersions')]
    public function testFromServerVersion(string $version, RedisVersionEnum $expected): void
    {
        self::assertSame($expected, RedisVersionEnum::fromServerVersion($version));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function unsupportedVersions(): iterable
    {
        yield 'too old' => ['5.0.14'];
        yield 'garbage' => ['unknown'];
        yield 'empty' => [''];
    }

    #[DataProvider('unsupportedVersions')]
    public function testFromServerVersionRejectsUnsupportedVersions(string $version): void
    {
        $this->expectException(VersionMismatchException::class);
        $this->expectExceptionMessage('version 6.0 or later is required');

        RedisVersionEnum::fromServerVersion($version);
    }

    public function testIsAtLeast(): void
    {
        self::assertTrue(RedisVersionEnum::GE70->isAtLeast(RedisVersionEnum::GE62));
        self::assertTrue(RedisVersionEnum::GE70->isAtLeast(RedisVersionEnum::GE70));
        self::assertFalse(RedisVersionEnum::GE70->isAtLeast(RedisVersionEnum::GE80));
        self::assertSame(['6.0', '6.2', '7.0', '8.0', '8.2', '8.4'], array_map(static fn (RedisVersionEnum $redisVersionEnum): string => $redisVersionEnum->value, RedisVersionEnum::cases()));
    }

    public function testCapabilitiesPerLevel(): void
    {
        $matrix = [];
        foreach (RedisVersionEnum::cases() as $level) {
            $redisCapabilities = RedisCapabilities::forVersion($level);
            self::assertSame($level, $redisCapabilities->version);
            $matrix[$level->value] = [
                $redisCapabilities->xpendingIdle,
                $redisCapabilities->xtrimMinId,
                $redisCapabilities->groupLag,
                $redisCapabilities->xackdel,
                $redisCapabilities->trimAcked,
                $redisCapabilities->readGroupClaim,
                $redisCapabilities->delex,
            ];
        }

        self::assertSame([
            '6.0' => [false, false, false, false, false, false, false],
            '6.2' => [true, true, false, false, false, false, false],
            '7.0' => [true, true, true, false, false, false, false],
            '8.0' => [true, true, true, false, false, false, false],
            '8.2' => [true, true, true, true, true, false, false],
            '8.4' => [true, true, true, true, true, true, true],
        ], $matrix);
    }

    public function testDetectorReadsTheServerVersion(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', "# Server\r\nredis_version:7.4.2\r\nredis_mode:standalone\r\n");

        self::assertSame(RedisVersionEnum::GE70, (new VersionDetector($recordingRedisClient))->resolve());
        self::assertSame([['INFO', 'server']], $recordingRedisClient->commands);
    }

    public function testDetectorFallsBackToTheBaselineWhenInfoIsForbidden(): void
    {
        $recordingLogger = new RecordingLogger();
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', CommandFailedException::rejected('INFO', "NOPERM this user has no permissions to run the 'info' command"));

        self::assertSame(RedisVersionEnum::GE60, (new VersionDetector($recordingRedisClient, $recordingLogger))->resolve());
        self::assertSame(['warning', 'warning'], $recordingLogger->levels());
    }

    public function testDetectorFallsBackWhenTheVersionIsMissing(): void
    {
        $recordingLogger = new RecordingLogger();
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', "# Server\r\nredis_mode:standalone\r\n");

        $versionDetector = new VersionDetector($recordingRedisClient, $recordingLogger);

        self::assertSame(RedisVersionEnum::GE60, $versionDetector->resolve());
        self::assertStringContainsString('did not report a recognizable redis_version', $recordingLogger->records[0]['message']);
        self::assertNull((new VersionDetector((new RecordingRedisClient())->reply('INFO', ['not', 'a', 'string'])))->serverVersion());
    }

    public function testDetectorFallsBackWhenTheVersionIsNotRecognizable(): void
    {
        $recordingLogger = new RecordingLogger();
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', "# Server\r\nredis_version:unknown\r\n");

        self::assertSame(RedisVersionEnum::GE60, (new VersionDetector($recordingRedisClient, $recordingLogger))->resolve());
        self::assertSame(['warning', 'warning'], $recordingLogger->levels(), 'same path as a forbidden INFO');
    }

    public function testDetectorAcceptsAVersionPrefixAndStillRejectsOldServers(): void
    {
        self::assertSame(RedisVersionEnum::GE80, (new VersionDetector((new RecordingRedisClient())->reply('INFO', "redis_version: v8.0.1\r\n")))->resolve());

        $this->expectException(VersionMismatchException::class);

        (new VersionDetector((new RecordingRedisClient())->reply('INFO', "redis_version:5.0.14\r\n")))->resolve();
    }

    public function testOverrideNotAboveTheServerIsAccepted(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', "redis_version:8.0.2\r\n");

        self::assertSame(RedisVersionEnum::GE62, (new VersionDetector($recordingRedisClient))->resolve(RedisVersionEnum::GE62));
    }

    public function testOverrideAboveTheServerIsRejected(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', "redis_version:7.2.4\r\n");

        $this->expectException(VersionMismatchException::class);
        $this->expectExceptionMessage('Requested Redis level 8.4 but the server reports 7.2.4');

        (new VersionDetector($recordingRedisClient))->resolve(RedisVersionEnum::GE84);
    }

    public function testOverrideWithoutVerificationDoesNotQueryTheServer(): void
    {
        $recordingRedisClient = new RecordingRedisClient();

        self::assertSame(RedisVersionEnum::GE84, (new VersionDetector($recordingRedisClient))->resolve(RedisVersionEnum::GE84, verify: false));
        self::assertSame([], $recordingRedisClient->commands);
    }

    public function testOverrideIsTrustedWhenTheVersionCannotBeRead(): void
    {
        $recordingRedisClient = (new RecordingRedisClient())->reply('INFO', CommandFailedException::rejected('INFO', 'NOPERM'));

        self::assertSame(RedisVersionEnum::GE82, (new VersionDetector($recordingRedisClient))->resolve(RedisVersionEnum::GE82));
    }
}
