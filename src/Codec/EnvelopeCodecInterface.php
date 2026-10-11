<?php

declare(strict_types=1);

namespace QueueBridge\Codec;

use QueueBridge\Exception\SerializationException;
use QueueBridge\Message\Message;

/**
 * Converte un Message nel wire format: una mappa piatta di stringhe, salvata come campi dello Stream
 * su Redis o come body + header su RabbitMQ. Pubblica perché serve anche a chi implementa un backend esterno.
 *
 * @api
 */
interface EnvelopeCodecInterface
{
    /**
     * Scrive sempre la versione più recente del formato.
     *
     * Complessità: O(t + h) nel codec fornito, t = byte del testo, h = byte degli header.
     *
     * @return array<string, string>
     */
    public function encode(Message $message): array;

    /**
     * Legge tutte le versioni supportate del formato.
     *
     * Complessità: O(t + h) nel codec fornito, t = byte del testo, h = byte degli header.
     *
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException se la versione è sconosciuta o un campo è assente o invalido
     */
    public function decode(array $fields): Message;
}
