<?php

declare(strict_types=1);

namespace snipershady\Queue\Middleware;

use DateTimeImmutable;
use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use snipershady\Queue\Message\Message;
use snipershady\Queue\System\SystemClock;
use Throwable;

/**
 * Logga inizio, esito e durata di ogni handler. Il contenuto di `text` non viene mai loggato, solo la sua
 * dimensione: può contenere dati personali.
 *
 * @api
 */
final readonly class LoggingMiddleware implements MiddlewareInterface
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        private LoggerInterface $logger,
        private ClockInterface $clock = new SystemClock(),
    ) {
    }

    /**
     * Complessità: O(1) più il costo di $next e del logger.
     */
    #[Override]
    public function process(Message $message, callable $next): void
    {
        $context = [
            'message_id' => $message->id->value,
            'channel' => $message->channel,
            'attempts' => $message->attempts,
            'text_bytes' => \strlen($message->text),
        ];
        $startedAt = $this->clock->now();
        $this->logger->info('Handling message {message_id} on channel {channel}.', $context);

        try {
            $next($message);
        } catch (Throwable $throwable) {
            $this->logger->warning('Handler failed for message {message_id}: {error}', [
                ...$context,
                'duration_ms' => $this->elapsedMs($startedAt),
                'error' => $throwable->getMessage(),
                'error_class' => $throwable::class,
                'exception' => $throwable,
            ]);

            throw $throwable;
        }

        $this->logger->info('Handled message {message_id} in {duration_ms} ms.', [...$context, 'duration_ms' => $this->elapsedMs($startedAt)]);
    }

    private function elapsedMs(DateTimeImmutable $startedAt): int
    {
        return (int) $this->clock->now()->format('Uv') - (int) $startedAt->format('Uv');
    }
}
