<?php

declare(strict_types=1);

namespace snipershady\Queue\Codec;

use snipershady\Queue\Exception\SerializationException;
use snipershady\Queue\Message\Message;

/**
 * Converte un Message nel wire format: una mappa piatta di stringhe, salvata come campi dello Stream
 * su Redis o come body + header su RabbitMQ (CLAUDE.md §8.4).
 *
 * @api
 */
interface EnvelopeCodecInterface
{
    /**
     * Scrive sempre la versione più recente del formato.
     *
     * @return array<string, string>
     */
    public function encode(Message $message): array;

    /**
     * Legge tutte le versioni supportate del formato.
     *
     * @param array<array-key, string> $fields
     *
     * @throws SerializationException se la versione è sconosciuta o un campo è assente o invalido
     */
    public function decode(array $fields): Message;
}
