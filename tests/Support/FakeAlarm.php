<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use Closure;
use Override;
use QueueBridge\System\AlarmInterface;

/**
 * Allarme controllato dal test: `fire()` esegue il callback programmato come farebbe SIGALRM.
 */
final class FakeAlarm implements AlarmInterface
{
    public ?int $scheduledSeconds = null;

    public int $cancellations = 0;

    /** @var ?Closure(): void */
    private ?Closure $onAlarm = null;

    public function __construct(private readonly bool $supported = true)
    {
    }

    #[Override]
    public function isSupported(): bool
    {
        return $this->supported;
    }

    #[Override]
    public function schedule(int $seconds, callable $onAlarm): void
    {
        $this->scheduledSeconds = $seconds;
        $this->onAlarm = $onAlarm(...);
    }

    #[Override]
    public function cancel(): void
    {
        ++$this->cancellations;
        $this->onAlarm = null;
    }

    public function fire(): void
    {
        if ($this->onAlarm instanceof Closure) {
            ($this->onAlarm)();
        }
    }
}
