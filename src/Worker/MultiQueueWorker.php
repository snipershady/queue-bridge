<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use InvalidArgumentException;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Exception\BackendException;
use snipershady\Queue\System\MemoryMeterInterface;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\SignalHandlerInterface;
use snipershady\Queue\System\SystemClock;

/**
 * Consuma più code a turno (un runOnce() per worker a ogni giro): un canale lento non blocca gli altri.
 *
 * I worker interni dovrebbero avere un `receiveTimeoutMs` basso (es. 100 ms), altrimenti una coda vuota fa
 * attendere le altre. I limiti di `$config` valgono per l'insieme; `maxMessages` può essere superato al più
 * di un batch. Su SIGTERM/SIGINT i worker interni vengono fermati e restituiscono i messaggi già ricevuti.
 *
 * @api
 */
final class MultiQueueWorker
{
    private readonly StopConditions $stopConditions;

    private bool $stopRequested = false;

    /**
     * @param non-empty-list<WorkerInterface> $workers worker già configurati con i propri handler
     * @param WorkerConfig                    $config  limiti e `stopWhenEmpty` dell'insieme
     *
     * @throws InvalidArgumentException se $workers è vuoto
     */
    public function __construct(
        private readonly array $workers,
        private readonly WorkerConfig $config = new WorkerConfig(),
        private readonly ClockInterface $clock = new SystemClock(),
        private readonly SignalHandlerInterface $signalHandler = new NullSignalHandler(),
        MemoryMeterInterface $memoryMeter = new NativeMemoryMeter(),
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
        if ([] === $workers) {
            throw new InvalidArgumentException('At least one worker is required.');
        }

        $this->stopConditions = new StopConditions($config, $clock, $memoryMeter);
    }

    /**
     * Esce dopo il giro corrente; i messaggi già ricevuti dai worker interni tornano in coda.
     */
    public function stop(): void
    {
        $this->stopRequested = true;
        foreach ($this->workers as $worker) {
            $worker->stop();
        }
    }

    /**
     * Un giro: un runOnce() per ogni worker.
     *
     * @throws BackendException
     *
     * @return int messaggi elaborati
     */
    public function runOnce(): int
    {
        $processed = 0;
        foreach ($this->workers as $worker) {
            if ($this->stopRequested) {
                break;
            }

            $processed += $worker->runOnce();
        }

        return $processed;
    }

    /**
     * @throws BackendException
     */
    public function run(): WorkerExitReason
    {
        $this->stopRequested = false;
        $this->signalHandler->onTerminate(function (int $signal): void {
            $this->logger->notice('Received signal {signal}: stopping after the current messages.', ['signal' => $signal]);
            $this->stop();
        });

        $startedAt = $this->clock->now();
        $processed = 0;

        try {
            while (true) {
                $reason = $this->stopRequested ? WorkerExitReason::StopRequested : $this->stopConditions->check($processed, $startedAt);
                if ($reason instanceof WorkerExitReason) {
                    break;
                }

                $round = $this->runOnce();
                $processed += $round;

                if (0 === $round && $this->config->stopWhenEmpty && !$this->stopRequested) {
                    $reason = WorkerExitReason::QueueEmpty;

                    break;
                }
            }
        } finally {
            $this->signalHandler->reset();
        }

        $this->logger->info('Multi-queue worker stopped ({reason}) after {processed} messages.', ['reason' => $reason->value, 'processed' => $processed]);

        return $reason;
    }
}
