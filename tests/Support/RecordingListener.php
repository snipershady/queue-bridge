<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

use Override;
use QueueBridge\Worker\Event\WorkerEventInterface;
use QueueBridge\Worker\Event\WorkerEventListenerInterface;

final class RecordingListener implements WorkerEventListenerInterface
{
    /** @var list<WorkerEventInterface> */
    public array $events = [];

    #[Override]
    public function onEvent(WorkerEventInterface $event): void
    {
        $this->events[] = $event;
    }

    /**
     * @return list<string> nomi brevi delle classi degli eventi, in ordine
     */
    public function names(): array
    {
        return array_map(static fn (WorkerEventInterface $workerEvent): string => \substr($workerEvent::class, (int) strrpos($workerEvent::class, '\\') + 1), $this->events);
    }

    /**
     * @template T of WorkerEventInterface
     *
     * @param class-string<T> $class
     *
     * @return list<T>
     */
    public function ofType(string $class): array
    {
        return array_values(array_filter($this->events, static fn (WorkerEventInterface $workerEvent): bool => $workerEvent instanceof $class));
    }
}
