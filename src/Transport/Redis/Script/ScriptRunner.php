<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Script;

use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;

/**
 * Esegue uno script con EVALSHA e, se il server non lo ha in cache (NOSCRIPT), con EVAL.
 *
 * @internal
 */
final readonly class ScriptRunner
{
    /**
     * Complessità: O(1).
     */
    public function __construct(private RedisClientInterface $client)
    {
    }

    /**
     * Complessità: O(s + a), s = lunghezza dello script (SHA1), a = argomenti; un round trip, due se lo script non è in
     * cache (NOSCRIPT).
     *
     * @param list<string> $keys
     * @param list<string> $arguments
     *
     * @throws CommandFailedException
     *
     * @return array<mixed>|int|string|null
     */
    public function run(string $script, array $keys, array $arguments): array|int|string|null
    {
        $tail = [(string) \count($keys), ...$keys, ...$arguments];

        try {
            return $this->client->execute(['EVALSHA', sha1($script), ...$tail]);
        } catch (CommandFailedException $commandFailedException) {
            if (!\str_contains($commandFailedException->getMessage(), 'NOSCRIPT')) {
                throw $commandFailedException;
            }

            return $this->client->execute(['EVAL', $script, ...$tail]);
        }
    }
}
