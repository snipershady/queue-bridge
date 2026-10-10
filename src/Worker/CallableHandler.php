<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use Closure;
use Override;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Text\DecodedText;

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
     * @param callable(Message, DecodedText): void $handler
     */
    public function __construct(callable $handler)
    {
        $this->handler = $handler(...);
    }

    #[Override]
    public function handle(Message $message, DecodedText $text): void
    {
        ($this->handler)($message, $text);
    }
}
