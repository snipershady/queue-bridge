<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use InvalidArgumentException;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Message\MessageRules;
use snipershady\Queue\Text\DecodedText;

/**
 * Canale → handler. Il canale speciale `*` registra l'handler di fallback per i canali senza handler.
 *
 * @internal
 */
final class HandlerRegistry
{
    public const string FALLBACK = '*';

    /** @var array<string, HandlerInterface> */
    private array $handlers = [];

    /**
     * Registra (o sostituisce) l'handler di un canale.
     *
     * @param HandlerInterface|callable(Message, DecodedText): void $handler
     *
     * @throws InvalidArgumentException se il canale non è valido
     */
    public function register(string $channel, HandlerInterface|callable $handler): void
    {
        if (self::FALLBACK !== $channel) {
            MessageRules::assertChannel($channel);
        }

        $this->handlers[$channel] = $handler instanceof HandlerInterface ? $handler : new CallableHandler($handler);
    }

    public function resolve(string $channel): ?HandlerInterface
    {
        return $this->handlers[$channel] ?? $this->handlers[self::FALLBACK] ?? null;
    }
}
