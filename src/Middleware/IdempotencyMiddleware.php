<?php

declare(strict_types=1);

namespace snipershady\Queue\Middleware;

use InvalidArgumentException;
use Override;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Idempotency\AcquireStatus;
use snipershady\Queue\Idempotency\IdempotencyStoreInterface;
use snipershady\Queue\Idempotency\LockToken;
use snipershady\Queue\Message\Message;
use Throwable;

/**
 * Esegue l'handler al più una volta per ID applicativo, anche se il messaggio viene consegnato più volte
 * (at-least-once). Se l'handler fallisce il lock viene rilasciato, così il retry può rieseguirlo.
 *
 * - Duplicato già completato: confermato senza eseguire l'handler.
 * - Duplicato in elaborazione presso un altro worker: **non** viene confermato (quel worker potrebbe
 *   fallire o morire, e il messaggio andrebbe perso) ma ritentato con una RetryableException, con ritardo
 *   pari al tempo residuo del lock. Al nuovo tentativo il messaggio risulta completato, oppure il lock è
 *   scaduto e l'handler viene eseguito. Il retry consuma un tentativo (`QueueConfig::$maxAttempts`): con
 *   `maxAttempts = 1` il duplicato va in DLQ, da cui si può riaccodare.
 *
 * @api
 */
final readonly class IdempotencyMiddleware implements MiddlewareInterface
{
    /**
     * Complessità: O(1).
     *
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

    /**
     * Complessità: O(1) più il costo di $next; due operazioni sullo store (un round trip ciascuna su Redis).
     *
     * @throws RetryableException se il messaggio è in elaborazione presso un altro worker
     */
    #[Override]
    public function process(Message $message, callable $next): void
    {
        $acquireResult = $this->store->acquire($message->id, $this->lockTtlMs);
        if (AcquireStatus::Completed === $acquireResult->status) {
            return;
        }

        $lockToken = $acquireResult->token;
        if (!$lockToken instanceof LockToken) {
            throw new RetryableException(\sprintf('Message %s is being processed by another worker: retrying when its lock expires.', $message->id->value), $acquireResult->lockRemainingMs);
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
