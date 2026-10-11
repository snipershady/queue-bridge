<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Client;

use Override;
use Predis\Client;
use Predis\ClientInterface;
use Predis\Command\RawCommand;
use Predis\Connection\ConnectionException as PredisConnectionException;
use Predis\Response\ErrorInterface;
use Predis\Response\ServerException;
use Predis\Response\Status;
use snipershady\Queue\Exception\CommandFailedException;
use snipershady\Queue\Exception\ConnectionException;

/**
 * Adapter per `predis/predis`. Predis si riconnette da solo al comando successivo: dopo un errore di
 * connessione il comando viene ritentato una volta.
 *
 * Pipeline: con un `Predis\Client` si usa la pipeline nativa. Se il client ha l'opzione `exceptions`
 * attiva, un errore in pipeline interrompe la lettura delle risposte e tutti i comandi risultano falliti:
 * per errori per singolo comando configurare `exceptions => false`.
 *
 * @api
 */
final readonly class PredisClient implements RedisClientInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(private ClientInterface $client)
    {
    }

    /**
     * Complessità: O(c + r), c = dimensione del comando, r = dimensione della risposta; un round trip (due dopo una
     * riconnessione).
     */
    #[Override]
    public function execute(array $command): array|int|string|null
    {
        return $this->withRetry(fn (): array|int|string|null => $this->toReply($command[0], $this->send($command)));
    }

    /**
     * Complessità: O(c + r) sull'intera pipeline; un round trip con `Predis\Client`, uno per comando con altri
     * `ClientInterface`.
     */
    #[Override]
    public function pipeline(array $commands): array
    {
        if (!$this->client instanceof Client) {
            return array_map(function (array $command): array|int|string|CommandFailedException {
                try {
                    return $this->execute($command) ?? CommandFailedException::rejected($command[0], 'nil reply');
                } catch (CommandFailedException $commandFailedException) {
                    return $commandFailedException;
                }
            }, $commands);
        }

        $client = $this->client;

        return $this->withRetry(static function () use ($client, $commands): array {
            try {
                $replies = $client->pipeline(static function (mixed $pipeline) use ($commands): void {
                    \assert($pipeline instanceof ClientInterface);
                    foreach ($commands as $command) {
                        $pipeline->executeCommand(RawCommand::create(...$command));
                    }
                });
            } catch (ServerException $serverException) {
                return array_map(static fn (array $command): CommandFailedException => CommandFailedException::rejected($command[0], $serverException->getMessage(), $serverException), $commands);
            }

            $results = [];
            foreach ($commands as $index => $command) {
                $reply = \is_array($replies) ? ($replies[$index] ?? null) : null;
                $results[] = match (true) {
                    $reply instanceof ErrorInterface => CommandFailedException::rejected($command[0], $reply->getMessage()),
                    null === $reply => CommandFailedException::rejected($command[0], 'nil reply'),
                    default => self::normalize($reply) ?? CommandFailedException::rejected($command[0], 'nil reply'),
                };
            }

            return $results;
        });
    }

    /**
     * @param non-empty-list<string> $command
     */
    private function send(array $command): mixed
    {
        try {
            return $this->client->executeCommand(RawCommand::create(...$command));
        } catch (ServerException $serverException) {
            throw CommandFailedException::rejected($command[0], $serverException->getMessage(), $serverException);
        }
    }

    /**
     * @return array<mixed>|int|string|null
     */
    private function toReply(string $command, mixed $reply): array|int|string|null
    {
        if ($reply instanceof ErrorInterface) {
            throw CommandFailedException::rejected($command, $reply->getMessage());
        }

        return self::normalize($reply);
    }

    /**
     * @template T
     *
     * @param callable(): T $operation
     *
     * @throws ConnectionException
     *
     * @return T
     */
    private function withRetry(callable $operation): mixed
    {
        try {
            return $operation();
        } catch (PredisConnectionException) {
            try {
                return $operation();
            } catch (PredisConnectionException $predisConnectionException) {
                throw ConnectionException::unreachable('Redis', $predisConnectionException->getMessage(), $predisConnectionException);
            }
        }
    }

    /**
     * @return array<mixed>|int|string|null
     */
    private static function normalize(mixed $reply): array|int|string|null
    {
        return match (true) {
            null === $reply => null,
            $reply instanceof Status => $reply->getPayload(),
            \is_array($reply) => array_map(self::normalize(...), $reply),
            \is_int($reply), \is_string($reply) => $reply,
            default => throw CommandFailedException::unexpectedReply('predis', 'a RESP2 value'),
        };
    }
}
