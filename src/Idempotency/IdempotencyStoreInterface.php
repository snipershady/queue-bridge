<?php

declare(strict_types=1);

namespace snipershady\Queue\Idempotency;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Message\MessageId;

/**
 * Memoria dei messaggi già elaborati, indipendente dal transport.
 *
 * @api
 */
interface IdempotencyStoreInterface
{
    /**
     * Acquisisce il lock "in elaborazione" per $ttlMs.
     *
     * @throws InvalidArgumentException se $ttlMs < 1
     * @throws BackendException
     *
     * @return ?LockToken null se il messaggio è già in elaborazione o già completato
     *
     * @phpstan-impure
     */
    public function acquire(MessageId $id, int $ttlMs): ?LockToken;

    /**
     * Marca il messaggio come completato per $ttlMs (sostituisce il lock).
     *
     * @throws InvalidArgumentException se $ttlMs < 1
     * @throws BackendException
     */
    public function complete(LockToken $token, int $ttlMs): void;

    /**
     * Rilascia il lock solo se è ancora quello di $token (confronto atomico), così un altro worker può riprovare.
     *
     * @throws BackendException
     */
    public function release(LockToken $token): void;
}
