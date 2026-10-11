<?php

declare(strict_types=1);

namespace snipershady\Queue\Dsn;

use InvalidArgumentException;

/**
 * DSN già scomposto: credenziali e path decodificati, opzioni dalla query string.
 *
 * @internal
 */
final readonly class Dsn
{
    /**
     * @param string                $path    database (Redis) o vhost (AMQP), senza "/" iniziale
     * @param array<string, string> $options
     */
    public function __construct(
        public StrategyEnum $strategy,
        public bool $secure,
        public string $host = '127.0.0.1',
        public ?int $port = null,
        public ?string $user = null,
        public ?string $password = null,
        public string $path = '',
        public array $options = [],
    ) {
    }

    public function option(string $name, string $default): string
    {
        return $this->options[$name] ?? $default;
    }

    /**
     * @throws InvalidArgumentException se il valore non è un intero >= $minimum
     */
    public function intOption(string $name, int $default, int $minimum = 0): int
    {
        if (!isset($this->options[$name])) {
            return $default;
        }

        $value = $this->options[$name];
        if (1 !== \preg_match('/^-?\d+$/D', $value) || (int) $value < $minimum) {
            throw new InvalidArgumentException(\sprintf('DSN option "%s" must be an integer >= %d, got "%s".', $name, $minimum, $value));
        }

        return (int) $value;
    }

    /**
     * @throws InvalidArgumentException se il valore non è un numero > 0
     */
    public function floatOption(string $name, float $default): float
    {
        if (!isset($this->options[$name])) {
            return $default;
        }

        $value = $this->options[$name];
        if (!\is_numeric($value) || (float) $value <= 0.0) {
            throw new InvalidArgumentException(\sprintf('DSN option "%s" must be a number > 0, got "%s".', $name, $value));
        }

        return (float) $value;
    }

    /**
     * @throws InvalidArgumentException se il valore non è un booleano (1/0, true/false, yes/no, on/off)
     */
    public function boolOption(string $name, bool $default): bool
    {
        if (!isset($this->options[$name])) {
            return $default;
        }

        return match (strtolower($this->options[$name])) {
            '1', 'true', 'yes', 'on' => true,
            '0', 'false', 'no', 'off' => false,
            default => throw new InvalidArgumentException(\sprintf('DSN option "%s" must be a boolean, got "%s".', $name, $this->options[$name])),
        };
    }
}
