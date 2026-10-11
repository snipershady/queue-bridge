<?php

declare(strict_types=1);

namespace snipershady\Queue\Cli;

use DateTimeInterface;
use Override;
use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;
use Stringable;
use Throwable;

/**
 * Logger PSR-3 su stderr. Verbosità 0: da warning in su; 1 (-v): da info; 2 (-vv): tutto.
 *
 * @internal
 */
final class ConsoleLogger extends AbstractLogger
{
    private const array SEVERITY = [
        LogLevel::DEBUG => 0,
        LogLevel::INFO => 1,
        LogLevel::NOTICE => 2,
        LogLevel::WARNING => 3,
        LogLevel::ERROR => 4,
        LogLevel::CRITICAL => 5,
        LogLevel::ALERT => 6,
        LogLevel::EMERGENCY => 7,
    ];

    /**
     * Complessità: O(1).
     */
    public function __construct(
        private readonly Output $output,
        private readonly int $verbosity = 0,
    ) {
    }

    /**
     * Complessità: O(m + c), m = lunghezza del messaggio, c = voci del contesto.
     *
     * @param array<mixed> $context
     */
    #[Override]
    public function log(mixed $level, string|Stringable $message, array $context = []): void
    {
        $level = \is_string($level) ? $level : LogLevel::INFO;
        $minimum = match (true) {
            $this->verbosity >= 2 => 0,
            1 === $this->verbosity => 1,
            default => 3,
        };

        if ((self::SEVERITY[$level] ?? 1) < $minimum) {
            return;
        }

        $replacements = [];
        foreach ($context as $key => $value) {
            $replacements['{'.$key.'}'] = match (true) {
                $value instanceof Throwable => $value::class,
                \is_scalar($value), $value instanceof Stringable => (string) $value,
                null === $value => 'null',
                $value instanceof DateTimeInterface => $value->format(\DATE_ATOM),
                default => '['.get_debug_type($value).']',
            };
        }

        $this->output->error(\sprintf('[%s] %s', $level, \strtr((string) $message, $replacements)));
    }
}
