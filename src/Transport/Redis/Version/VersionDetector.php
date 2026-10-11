<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Version;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\VersionMismatchException;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;

/**
 * Sceglie il livello Redis: rilevato da `INFO server` (una volta), oppure forzato.
 *
 * @internal
 */
final readonly class VersionDetector
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        private RedisClientInterface $client,
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    /**
     * Versione del server da `INFO server`; null se il comando è vietato (ACL) o la risposta non la contiene.
     *
     * Complessità: O(i), i = dimensione della risposta di INFO server; un round trip.
     */
    public function serverVersion(): ?string
    {
        try {
            $info = $this->client->execute(['INFO', 'server']);
        } catch (CommandFailedException $commandFailedException) {
            $this->logger->warning('Cannot read the Redis version ({error}).', ['error' => $commandFailedException->getMessage()]);

            return null;
        }

        if (\is_string($info) && 1 === \preg_match('/^redis_version:([^\r\n]+)/m', $info, $matches)) {
            return \trim($matches[1]);
        }

        $this->logger->warning('INFO server did not report redis_version.');

        return null;
    }

    /**
     * Complessità: O(i), i = dimensione della risposta di INFO server; al più un round trip.
     *
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
