<?php

declare(strict_types=1);

namespace QueueBridge\Middleware;

use InvalidArgumentException;
use Override;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use QueueBridge\Exception\RetryableException;
use QueueBridge\Message\Message;
use QueueBridge\System\AlarmInterface;

/**
 * Interrompe un handler che supera $seconds lanciando una RetryableException (un timeout è di norma
 * transitorio). Richiede un allarme supportato (`PcntlAlarm`); altrimenti lo segnala nel log e non
 * applica limiti. Un handler che cattura ogni Throwable può inghiottire il timeout.
 *
 * @api
 */
final readonly class TimeoutMiddleware implements MiddlewareInterface
{
    /**
     * Complessità: O(1).
     *
     * @throws InvalidArgumentException se $seconds < 1
     */
    public function __construct(
        private AlarmInterface $alarm,
        private int $seconds,
        LoggerInterface $logger = new NullLogger(),
    ) {
        if ($seconds < 1) {
            throw new InvalidArgumentException(\sprintf('Timeout must be >= 1 second, got %d.', $seconds));
        }

        if (!$alarm->isSupported()) {
            $logger->warning('Handler timeout of {seconds}s is not enforced: no alarm support (ext-pcntl missing).', ['seconds' => $seconds]);
        }
    }

    /**
     * Complessità: O(1).
     */
    public function timeoutSeconds(): int
    {
        return $this->seconds;
    }

    /**
     * Complessità: O(1) più il costo di $next.
     */
    #[Override]
    public function process(Message $message, callable $next): void
    {
        if (!$this->alarm->isSupported()) {
            $next($message);

            return;
        }

        $seconds = $this->seconds;
        $this->alarm->schedule($seconds, static function () use ($seconds): never {
            throw new RetryableException(\sprintf('Handler timed out after %d seconds.', $seconds));
        });

        try {
            $next($message);
        } finally {
            $this->alarm->cancel();
        }
    }
}
