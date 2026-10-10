<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport;

use InvalidArgumentException;

/**
 * @api
 */
final readonly class QueueStats
{
    /**
     * @param int  $ready       messaggi pronti, non ancora consegnati
     * @param int  $inFlight    messaggi consegnati e non ancora confermati
     * @param int  $delayed     messaggi differiti non ancora disponibili
     * @param int  $deadLetters messaggi in DLQ
     * @param ?int $lag         messaggi mai letti dal gruppo di consumer; null se il backend non lo fornisce
     * @param ?int $consumers   consumer collegati; null se il backend non lo fornisce
     *
     * @throws InvalidArgumentException se un contatore è negativo
     */
    public function __construct(
        public int $ready,
        public int $inFlight,
        public int $delayed,
        public int $deadLetters,
        public ?int $lag = null,
        public ?int $consumers = null,
    ) {
        foreach (['ready' => $ready, 'inFlight' => $inFlight, 'delayed' => $delayed, 'deadLetters' => $deadLetters, 'lag' => $lag, 'consumers' => $consumers] as $name => $value) {
            if (null !== $value && $value < 0) {
                throw new InvalidArgumentException(\sprintf('%s must be >= 0, got %d.', $name, $value));
            }
        }
    }
}
