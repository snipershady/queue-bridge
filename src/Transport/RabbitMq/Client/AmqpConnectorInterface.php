<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\RabbitMq\Client;

use snipershady\Queue\Exception\ConnectionException;

/**
 * Fornisce il canale corrente e lo riapre quando serve: un canale AMQP muore a ogni errore.
 *
 * @api
 */
interface AmqpConnectorInterface
{
    /**
     * Il canale aperto; ne apre uno nuovo (riconnettendo, se possibile) quando il precedente è chiuso.
     *
     * @throws ConnectionException
     */
    public function channel(): AmqpChannelInterface;

    /**
     * Identifica il canale corrente: cambia ogni volta che ne viene aperto uno nuovo. Le delivery tag
     * valgono solo sul canale che le ha emesse.
     */
    public function generation(): int;

    /**
     * Abbandona il canale corrente (i messaggi non confermati tornano in coda); il prossimo channel() ne apre uno nuovo.
     */
    public function reset(): void;
}
