<?php

declare(strict_types=1);

namespace QueueBridge\Idempotency;

use InvalidArgumentException;
use QueueBridge\Exception\BackendException;
use QueueBridge\Message\MessageId;

/**
 * Memoria dei messaggi già elaborati, indipendente dal transport.
 *
 * @api
 */
interface IdempotencyStoreInterface
{
    /**
     * Acquisisce il lock "in elaborazione" per $ttlMs. Il controllo e l'acquisizione sono atomici: lo stesso
     * messaggio non risulta mai `Acquired` per due worker con lock validi nello stesso momento.
     *
     * Le implementazioni devono distinguere "in elaborazione" da "completato": confermare un messaggio
     * ancora in elaborazione altrove lo perderebbe se quel worker poi fallisse o morisse.
     *
     * Complessità: O(1) negli store forniti (uno script su Redis, un round trip).
     *
     * @throws InvalidArgumentException se $ttlMs < 1
     * @throws BackendException
     *
     * @phpstan-impure
     */
    public function acquire(MessageId $id, int $ttlMs): AcquireResult;

    /**
     * Marca il messaggio come completato per $ttlMs (sostituisce il lock).
     *
     * Complessità: O(1) negli store forniti (un round trip su Redis).
     *
     * @throws InvalidArgumentException se $ttlMs < 1
     * @throws BackendException
     */
    public function complete(LockToken $token, int $ttlMs): void;

    /**
     * Rilascia il lock solo se è ancora quello di $token (confronto atomico), così un altro worker può riprovare.
     *
     * Complessità: O(1) negli store forniti (un round trip su Redis).
     *
     * @throws BackendException
     */
    public function release(LockToken $token): void;
}
