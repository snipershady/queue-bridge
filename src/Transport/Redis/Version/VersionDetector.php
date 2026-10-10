<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Version;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\VersionMismatchException;
use snipershady\Queue\Transport\Redis\Client\RedisClientInterface;

/**
 * Sceglie il livello Redis: rilevato da `INFO server` (una volta), oppure forzato (CLAUDE.md §7.1).
 *
 * @api
 */
final readonly class VersionDetector
{
    public function __construct(
        private RedisClientInterface $client,
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    /**
     * Versione del server da `INFO server`; null se il comando è vietato (ACL) o la risposta non la contiene.
     */
    public function serverVersion(): ?string
    {
        try {
            $info = $this->client->execute(['INFO', 'server']);
        } catch (CommandFailedException $commandFailedException) {
            $this->logger->warning('Cannot read the Redis version ({error}).', ['error' => $commandFailedException->getMessage()]);

            return null;
        }

        if (\is_string($info) && 1 === preg_match('/^redis_version:([^\r\n]+)/m', $info, $matches)) {
            return trim($matches[1]);
        }

        $this->logger->warning('INFO server did not report redis_version.');

        return null;
    }

    /**
     * @param ?RedisVersionEnum $override livello forzato; null = auto-rilevamento
     * @param bool              $verify   con un override, fallisce se supera la versione del server
     *
     * @throws VersionMismatchException
     */
    public function resolve(?RedisVersionEnum $override = null, bool $verify = true): RedisVersionEnum
    {
        if ($override instanceof RedisVersionEnum && !$verify) {
            return $override;
        }

        $server = $this->serverVersion();
        if (null === $server) {
            if ($override instanceof RedisVersionEnum) {
                return $override;
            }

            $this->logger->warning('Falling back to the Redis {level} baseline.', ['level' => RedisVersionEnum::GE60->value]);

            return RedisVersionEnum::GE60;
        }

        $detected = RedisVersionEnum::fromServerVersion($server);
        if (!$override instanceof RedisVersionEnum) {
            return $detected;
        }

        if (!$detected->isAtLeast($override)) {
            throw VersionMismatchException::overrideAboveServer($override->value, $server);
        }

        return $override;
    }
}
