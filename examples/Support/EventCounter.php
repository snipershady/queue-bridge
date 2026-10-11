<?php

declare(strict_types=1);

namespace QueueBridgeExamples;

use Override;
use QueueBridge\Worker\Event\WorkerEventInterface;
use QueueBridge\Worker\Event\WorkerEventListenerInterface;
use ReflectionClass;

/**
 * Counts the worker events by type: the place where an application would update Prometheus/StatsD metrics.
 */
final class EventCounter implements WorkerEventListenerInterface
{
    /** @var array<string, int> */
    public array $counts = [];

    #[Override]
    public function onEvent(WorkerEventInterface $event): void
    {
        $name = (new ReflectionClass($event))->getShortName();
        $this->counts[$name] = ($this->counts[$name] ?? 0) + 1;
    }
}
