<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

/**
 * @api
 */
enum WorkerExitReason: string
{
    /**
     * SIGTERM/SIGINT ricevuto o `stop()` chiamato.
     */
    case StopRequested = 'stop_requested';

    /**
     * Raggiunto `WorkerConfig::$maxMessages`.
     */
    case MessageLimitReached = 'message_limit_reached';

    /**
     * Raggiunto `WorkerConfig::$maxRuntimeSeconds`.
     */
    case TimeLimitReached = 'time_limit_reached';

    /**
     * Superato `WorkerConfig::$memoryLimitMb`.
     */
    case MemoryLimitReached = 'memory_limit_reached';

    /**
     * Coda vuota con `WorkerConfig::$stopWhenEmpty`.
     */
    case QueueEmpty = 'queue_empty';
}
