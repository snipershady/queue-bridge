<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Support;

use Closure;
use Override;
use snipershady\Queue\System\SignalHandlerInterface;

final class FakeSignalHandler implements SignalHandlerInterface
{
    public int $resets = 0;

    /** @var ?Closure(int): void */
    private ?Closure $onTerminate = null;

    #[Override]
    public function onTerminate(callable $onTerminate): void
    {
        $this->onTerminate = $onTerminate(...);
    }

    #[Override]
    public function reset(): void
    {
        ++$this->resets;
        $this->onTerminate = null;
    }

    public function send(int $signal = \SIGTERM): void
    {
        if ($this->onTerminate instanceof Closure) {
            ($this->onTerminate)($signal);
        }
    }
}
