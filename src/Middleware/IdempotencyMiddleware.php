<?php

declare(strict_types=1);

namespace snipershady\Queue\Middleware;

use InvalidArgumentException;
use Override;
use snipershady\Queue\Idempotency\IdempotencyStoreInterface;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\Message;
use Throwable;

/**
 * Esegue l'handler al più una volta per ID applicativo, anche se il messaggio viene consegnato più volte
 * (at-least-once). Un duplicato già completato o in elaborazione altrove viene confermato senza eseguire
 * l'handler; se l'handler fallisce il lock viene rilasciato, così il retry può rieseguirlo.
 *
 * @api
 */
final readonly class IdempotencyMiddleware implements MiddlewareInterface
{
    /**
     * @param int $lockTtlMs durata del lock "in elaborazione": deve superare il tempo massimo dell'handler
     * @param int $doneTtlMs per quanto un messaggio completato viene ricordato
     *
     * @throws InvalidArgumentException se un TTL è < 1
     */
    public function __construct(
        private IdempotencyStoreInterface $store,
        private int $lockTtlMs = 300_000,
        private int $doneTtlMs = 86_400_000,
    ) {
        if ($lockTtlMs < 1 || $doneTtlMs < 1) {
            throw new InvalidArgumentException(\sprintf('TTLs must be >= 1 ms, got %d and %d.', $lockTtlMs, $doneTtlMs));
        }
    }

    #[Override]
    public function process(Message $message, callable $next): void
    {
        $lockToken = $this->store->acquire($message->id, $this->lockTtlMs);
        if (!$lockToken instanceof LockToken) {
            return;
        }

        try {
            $next($message);
        } catch (Throwable $throwable) {
            $this->store->release($lockToken);

            throw $throwable;
        }

        $this->store->complete($lockToken, $this->doneTtlMs);
    }
}
