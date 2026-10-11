<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use InvalidArgumentException;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Text\DecodedText;

/**
 * @api
 */
interface WorkerInterface
{
    /**
     * Registra l'handler di un canale; `*` registra il fallback.
     *
     * @param HandlerInterface|callable(Message, DecodedText): void $handler
     *
     * @throws InvalidArgumentException se il canale non è valido
     */
    public function on(string $channel, HandlerInterface|callable $handler): static;

    /**
     * Elabora messaggi finché una condizione di uscita non si verifica. Le eccezioni degli handler non lo
     * fermano mai; quelle del backend sì (il supervisor riavvia il processo).
     *
     * @throws BackendException
     */
    public function run(): WorkerExitReason;

    /**
     * Un ciclo: manutenzione se dovuta, una ricezione, elaborazione del batch.
     *
     * @throws BackendException
     *
     * @return int messaggi elaborati (confermati, ritentati, in DLQ o scartati)
     */
    public function runOnce(): int;

    /**
     * Chiede al worker di uscire dopo il messaggio corrente; i messaggi già ricevuti e non elaborati tornano
     * in coda con release(). Sicuro da chiamare da un signal handler o da un altro worker.
     */
    public function stop(): void;
}
