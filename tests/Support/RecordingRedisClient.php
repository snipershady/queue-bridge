<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use Closure;
use Override;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Transport\Redis\Client\RedisClientInterface;

/**
 * Client Redis finto: registra i comandi e risponde con le risposte programmate per nome di comando
 * (in ordine FIFO), oppure con il responder di default. Una risposta CommandFailedException viene lanciata.
 */
final class RecordingRedisClient implements RedisClientInterface
{
    /** @var list<non-empty-list<string>> */
    public array $commands = [];

    /** @var list<non-empty-list<non-empty-list<string>>> */
    public array $pipelines = [];

    /** @var array<string, list<array<mixed>|int|string|CommandFailedException|null>> */
    private array $replies = [];

    /** @var Closure(non-empty-list<string>): (array<mixed>|int|string|CommandFailedException|null) */
    private readonly Closure $default;

    /**
     * @param ?Closure(non-empty-list<string>): (array<mixed>|int|string|CommandFailedException|null) $default
     */
    public function __construct(?Closure $default = null)
    {
        $this->default = $default ?? static fn (array $command): ?string => null;
    }

    /**
     * Programma la prossima risposta per $command (es. "XREADGROUP", "EVALSHA").
     *
     * @param array<mixed>|int|string|CommandFailedException|null $reply
     */
    public function reply(string $command, array|int|string|CommandFailedException|null $reply): self
    {
        $this->replies[$command][] = $reply;

        return $this;
    }

    #[Override]
    public function execute(array $command): array|int|string|null
    {
        $this->commands[] = $command;
        $reply = isset($this->replies[$command[0]]) && [] !== $this->replies[$command[0]]
            ? array_shift($this->replies[$command[0]])
            : ($this->default)($command);

        if ($reply instanceof CommandFailedException) {
            throw $reply;
        }

        return $reply;
    }

    #[Override]
    public function pipeline(array $commands): array
    {
        $this->pipelines[] = $commands;

        $results = [];
        foreach ($commands as $command) {
            $reply = isset($this->replies[$command[0]]) && [] !== $this->replies[$command[0]]
                ? array_shift($this->replies[$command[0]])
                : ($this->default)($command);
            $results[] = $reply ?? CommandFailedException::rejected($command[0], 'nil');
        }

        return $results;
    }

    /**
     * @return list<string> nomi dei comandi eseguiti, in ordine
     */
    public function names(): array
    {
        return array_map(static fn (array $command): string => $command[0], $this->commands);
    }

    /**
     * @return list<non-empty-list<string>> comandi con il nome indicato
     */
    public function named(string $name): array
    {
        return array_values(array_filter($this->commands, static fn (array $command): bool => $command[0] === $name));
    }
}
