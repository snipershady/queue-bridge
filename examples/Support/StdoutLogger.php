<?php

declare(strict_types=1);

namespace QueueBridgeExamples;

use Override;
use Psr\Log\AbstractLogger;
use Stringable;

/**
 * Minimal PSR-3 logger for the examples: in an application use Monolog or your framework's logger.
 */
final class StdoutLogger extends AbstractLogger
{
    /**
     * @param array<mixed> $context
     */
    #[Override]
    public function log(mixed $level, string|Stringable $message, array $context = []): void
    {
        $replacements = [];
        foreach ($context as $key => $value) {
            $replacements['{'.$key.'}'] = \is_scalar($value) || $value instanceof Stringable ? (string) $value : get_debug_type($value);
        }

        printf("  [%s] %s\n", \is_string($level) ? $level : 'info', strtr((string) $message, $replacements));
    }
}
