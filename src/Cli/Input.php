<?php

declare(strict_types=1);

namespace QueueBridge\Cli;

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
     * Complessità: O(a), a = numero di argomenti.
     *
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

            if (1 === \preg_match('/^-v+$/D', $argument)) {
                // -v info, -vv (o più) debug.
                $verbosity = \max($verbosity, \min(2, \strlen($argument) - 1));

                continue;
            }

            if ('-h' === $argument) {
                $options['help'] = true;

                continue;
            }

            if (!\str_starts_with($argument, '--')) {
                if (null !== $command) {
                    throw new UsageException(\sprintf('Unexpected argument "%s".', $argument));
                }

                $command = $argument;

                continue;
            }

            $parts = explode('=', \substr($argument, 2), 2);

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
                if (null === $value) {
                    throw new UsageException(\sprintf('Option --%s requires a value.', $name));
                }

                if (\str_starts_with($value, '-')) {
                    throw new UsageException(\sprintf('Option --%s requires a value (got "%s": write --%s=<value> for a value starting with "-").', $name, $value, $name));
                }

                ++$i;
            }

            $options[$name] = $value;
        }

        return new self($command, $options, $verbosity);
    }

    /**
     * Rifiuta le opzioni non previste dal comando: un nome scritto male (`--max-message=5`) non deve far girare
     * il comando con il valore di default.
     *
     * Complessità: O(o·a), o = opzioni presenti, a = opzioni ammesse.
     *
     * @param list<string> $allowed
     *
     * @throws UsageException
     */
    public function assertOptions(string $command, array $allowed): void
    {
        foreach (array_keys($this->options) as $name) {
            if (!\in_array($name, $allowed, strict: true)) {
                throw new UsageException(\sprintf('Unknown option --%s for "%s". Run with --help for the list of options.', $name, $command));
            }
        }
    }

    /**
     * Complessità: O(1).
     */
    public function flag(string $name): bool
    {
        return true === ($this->options[$name] ?? false);
    }

    /**
     * Complessità: O(1).
     */
    public function value(string $name): ?string
    {
        $value = $this->options[$name] ?? null;

        return \is_string($value) ? $value : null;
    }

    /**
     * Complessità: O(v), v = lunghezza del valore.
     *
     * @throws UsageException se il valore non è un intero >= $minimum
     */
    public function int(string $name, ?int $default, int $minimum = 1): ?int
    {
        $value = $this->value($name);
        if (null === $value) {
            return $default;
        }

        if (1 !== \preg_match('/^\d+$/D', $value) || (int) $value < $minimum) {
            throw new UsageException(\sprintf('Option --%s must be an integer >= %d, got "%s".', $name, $minimum, $value));
        }

        return (int) $value;
    }
}
