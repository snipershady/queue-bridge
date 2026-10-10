<?php

declare(strict_types=1);

namespace snipershady\Queue\Cli;

/**
 * Argomenti della CLI: `comando [--opzione=valore | --opzione valore | --flag | -v | -vv]`.
 *
 * @internal
 */
final readonly class Input
{
    /** Opzioni senza valore. */
    public const array FLAGS = ['help', 'json', 'stop-when-empty', 'force'];

    /**
     * @param array<string, string|true> $options
     */
    private function __construct(
        public ?string $command,
        public array $options,
        public int $verbosity,
    ) {
    }

    /**
     * @param list<string> $arguments argomenti senza il nome dello script
     *
     * @throws UsageException
     */
    public static function parse(array $arguments): self
    {
        $command = null;
        $options = [];
        $verbosity = 0;

        for ($i = 0, $count = \count($arguments); $i < $count; ++$i) {
            $argument = $arguments[$i];

            if ('-v' === $argument || '-vv' === $argument) {
                $verbosity = max($verbosity, \strlen($argument) - 1);

                continue;
            }

            if ('-h' === $argument) {
                $options['help'] = true;

                continue;
            }

            if (!str_starts_with($argument, '--')) {
                if (null !== $command) {
                    throw new UsageException(\sprintf('Unexpected argument "%s".', $argument));
                }

                $command = $argument;

                continue;
            }

            $parts = explode('=', substr($argument, 2), 2);

            $name = $parts[0];

            $value = $parts[1] ?? null;
            if ('' === $name) {
                throw new UsageException('Empty option name.');
            }

            if (\in_array($name, self::FLAGS, strict: true)) {
                if (null !== $value) {
                    throw new UsageException(\sprintf('Option --%s does not take a value.', $name));
                }

                $options[$name] = true;

                continue;
            }

            if (null === $value) {
                $value = $arguments[$i + 1] ?? null;
                if (null === $value || str_starts_with($value, '-')) {
                    throw new UsageException(\sprintf('Option --%s requires a value.', $name));
                }

                ++$i;
            }

            $options[$name] = $value;
        }

        return new self($command, $options, $verbosity);
    }

    public function flag(string $name): bool
    {
        return true === ($this->options[$name] ?? false);
    }

    public function value(string $name): ?string
    {
        $value = $this->options[$name] ?? null;

        return \is_string($value) ? $value : null;
    }

    /**
     * @throws UsageException se il valore non è un intero >= $minimum
     */
    public function int(string $name, ?int $default, int $minimum = 1): ?int
    {
        $value = $this->value($name);
        if (null === $value) {
            return $default;
        }

        if (1 !== preg_match('/^\d+$/D', $value) || (int) $value < $minimum) {
            throw new UsageException(\sprintf('Option --%s must be an integer >= %d, got "%s".', $name, $minimum, $value));
        }

        return (int) $value;
    }
}
