<?php

declare(strict_types=1);

namespace QueueBridge\Worker;

use Closure;
use Override;
use QueueBridge\Message\Message;
use QueueBridge\Text\DecodedText;

/**
 * Adatta una closure a HandlerInterface.
 *
 * @internal
 */
final readonly class CallableHandler implements HandlerInterface
{
    /** @var Closure(Message, DecodedText): void */
    private Closure $handler;

    /**
     * Complessità: O(1).
     *
     * @param callable(Message, DecodedText): void $handler
     */
    public function __construct(callable $handler)
    {
        $this->handler = $handler(...);
    }

    /**
     * Complessità: O(1) più il costo della callable.
     */
    #[Override]
    public function handle(Message $message, DecodedText $text): void
    {
        ($this->handler)($message, $text);
    }
}
