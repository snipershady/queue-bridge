<?php

declare(strict_types=1);

namespace QueueBridge\Cli;

use InvalidArgumentException;

/**
 * @internal
 */
final readonly class Output
{
    /** @var resource */
    private mixed $stdout;

    /** @var resource */
    private mixed $stderr;

    /**
     * Complessità: O(1).
     *
     * @param resource $stdout
     * @param resource $stderr
     *
     * @throws InvalidArgumentException se uno stream non è valido
     */
    public function __construct(mixed $stdout, mixed $stderr)
    {
        if (!\is_resource($stdout) || !\is_resource($stderr)) {
            throw new InvalidArgumentException('Output streams must be open resources.');
        }

        $this->stdout = $stdout;
        $this->stderr = $stderr;
    }

    /**
     * Complessità: O(l), l = lunghezza della riga.
     */
    public function line(string $line = ''): void
    {
        fwrite($this->stdout, $line.\PHP_EOL);
    }

    /**
     * Complessità: O(l), l = lunghezza della riga.
     */
    public function error(string $line): void
    {
        fwrite($this->stderr, $line.\PHP_EOL);
    }
}
