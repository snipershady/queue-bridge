<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis;

use QueueBridge\Exception\CommandFailedException;

/**
 * Restringe le risposte RESP2 ai tipi attesi, fallendo con un errore esplicito se il server risponde in un
 * formato diverso (versione o fork inatteso).
 *
 * @internal
 */
final class RedisReply
{
    /**
     * Complessità: O(1).
     *
     * @throws CommandFailedException
     *
     * @return list<mixed>
     */
    public static function list(mixed $reply, string $context): array
    {
        if (!\is_array($reply) || !array_is_list($reply)) {
            throw CommandFailedException::unexpectedReply($context, 'an array');
        }

        return $reply;
    }

    /**
     * Complessità: O(1).
     *
     * @throws CommandFailedException
     */
    public static function string(mixed $reply, string $context): string
    {
        if (\is_int($reply)) {
            return (string) $reply;
        }

        if (!\is_string($reply)) {
            throw CommandFailedException::unexpectedReply($context, 'a string');
        }

        return $reply;
    }

    /**
     * Complessità: O(v), v = cifre del valore.
     *
     * @throws CommandFailedException
     */
    public static function int(mixed $reply, string $context): int
    {
        if (\is_string($reply) && 1 === \preg_match('/^-?\d+$/D', $reply)) {
            return (int) $reply;
        }

        if (!\is_int($reply)) {
            throw CommandFailedException::unexpectedReply($context, 'an integer');
        }

        return $reply;
    }

    /**
     * Lista piatta [campo, valore, campo, valore, ...] → mappa.
     *
     * Complessità: O(n), n = elementi della risposta.
     *
     * @throws CommandFailedException
     *
     * @return array<string, mixed>
     */
    public static function pairs(mixed $reply, string $context): array
    {
        $list = self::list($reply, $context);
        if (0 !== \count($list) % 2) {
            throw CommandFailedException::unexpectedReply($context, 'an even number of elements');
        }

        $map = [];
        for ($i = 0, $count = \count($list); $i < $count; $i += 2) {
            $map[self::string($list[$i], $context)] = $list[$i + 1];
        }

        return $map;
    }

    /**
     * Come pairs(), con valori stringa (campi di un'entry dello stream).
     *
     * Complessità: O(n), n = elementi della risposta.
     *
     * @throws CommandFailedException
     *
     * @return array<string, string>
     */
    public static function fields(mixed $reply, string $context): array
    {
        return array_map(static fn (mixed $value): string => self::string($value, $context), self::pairs($reply, $context));
    }
}
