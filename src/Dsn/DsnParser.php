<?php

declare(strict_types=1);

namespace QueueBridge\Dsn;

use InvalidArgumentException;

/**
 * `redis[s]://[user:password@]host[:port][/db][?opzioni]`, `amqp[s]://user:password@host[:port][/vhost][?opzioni]`,
 * `memory://`. Credenziali e vhost vanno codificati come URL (es. vhost "/" = "%2F").
 *
 * @internal
 */
final class DsnParser
{
    /**
     * Complessità: O(d), d = lunghezza del DSN.
     *
     * @throws InvalidArgumentException se il DSN non è valido
     */
    public static function parse(string $dsn): Dsn
    {
        $scheme = \strstr($dsn, '://', before_needle: true);
        if (false === $scheme || '' === $scheme) {
            throw new InvalidArgumentException(\sprintf('Invalid DSN "%s": expected scheme://...', self::redact($dsn)));
        }

        $strategy = StrategyEnum::fromScheme($scheme);
        if (StrategyEnum::InMemory === $strategy) {
            if ($scheme.'://' !== $dsn) {
                throw new InvalidArgumentException(\sprintf('Invalid DSN "%s": the in-memory backend takes no host, path or options.', self::redact($dsn)));
            }

            return new Dsn($strategy, secure: false);
        }

        $parts = parse_url($dsn);
        if (false === $parts || !isset($parts['host'])) {
            throw new InvalidArgumentException(\sprintf('Invalid DSN "%s": a host is required.', self::redact($dsn)));
        }

        return new Dsn(
            strategy: $strategy,
            secure: \in_array(strtolower($scheme), ['rediss', 'amqps'], strict: true),
            host: $parts['host'],
            port: $parts['port'] ?? null,
            user: isset($parts['user']) && '' !== $parts['user'] ? urldecode($parts['user']) : null,
            password: isset($parts['pass']) ? urldecode($parts['pass']) : null,
            path: urldecode(ltrim($parts['path'] ?? '', '/')),
            options: self::options($parts['query'] ?? '', $dsn),
        );
    }

    /**
     * Nasconde la password nei messaggi d'errore.
     *
     * Complessità: O(d), d = lunghezza del DSN.
     */
    public static function redact(string $dsn): string
    {
        // Fino all'ultima "@" prima della query: una password con "@" non codificata non trapela.
        return (string) \preg_replace('#(://[^:/@]*:)[^?\#]*@#', '$1***@', $dsn);
    }

    /**
     * Query string → opzioni, senza parse_str (che riscrive "." e spazi in "_" e accetta array): un nome
     * scritto male deve restare riconoscibile per essere rifiutato.
     *
     * @throws InvalidArgumentException se un'opzione è vuota o ripetuta
     *
     * @return array<string, string>
     */
    private static function options(string $query, string $dsn): array
    {
        $options = [];
        foreach (explode('&', $query) as $pair) {
            if ('' === $pair) {
                continue;
            }

            [$name, $value] = explode('=', $pair, 2) + [1 => ''];
            $name = urldecode($name);
            if ('' === $name || \array_key_exists($name, $options)) {
                throw new InvalidArgumentException(\sprintf('Invalid DSN "%s": empty or repeated option "%s".', self::redact($dsn), $name));
            }

            $options[$name] = urldecode($value);
        }

        return $options;
    }
}
