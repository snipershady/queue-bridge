<?php

declare(strict_types=1);

namespace snipershady\Queue\Dsn;

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
     * @throws InvalidArgumentException se il DSN non è valido
     */
    public static function parse(string $dsn): Dsn
    {
        $scheme = strstr($dsn, '://', before_needle: true);
        if (false === $scheme || '' === $scheme) {
            throw new InvalidArgumentException(\sprintf('Invalid DSN "%s": expected scheme://...', self::redact($dsn)));
        }

        $strategy = StrategyEnum::fromScheme($scheme);
        if (StrategyEnum::InMemory === $strategy) {
            return new Dsn($strategy, secure: false);
        }

        $parts = parse_url($dsn);
        if (false === $parts || !isset($parts['host'])) {
            throw new InvalidArgumentException(\sprintf('Invalid DSN "%s": a host is required.', self::redact($dsn)));
        }

        parse_str($parts['query'] ?? '', $query);
        $options = [];
        foreach ($query as $name => $value) {
            if (\is_string($value)) {
                $options[(string) $name] = $value;
            }
        }

        return new Dsn(
            strategy: $strategy,
            secure: \in_array(strtolower($scheme), ['rediss', 'amqps'], strict: true),
            host: $parts['host'],
            port: $parts['port'] ?? null,
            user: isset($parts['user']) && '' !== $parts['user'] ? urldecode($parts['user']) : null,
            password: isset($parts['pass']) ? urldecode($parts['pass']) : null,
            path: urldecode(ltrim($parts['path'] ?? '', '/')),
            options: $options,
        );
    }

    /**
     * Nasconde la password nei messaggi d'errore.
     */
    public static function redact(string $dsn): string
    {
        return (string) preg_replace('#(://[^:/@]*:)[^@/]*@#', '$1***@', $dsn);
    }
}
