<?php

declare(strict_types=1);

namespace snipershady\Queue\Worker;

use DateTimeImmutable;
use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\RetryableException;
use snipershady\Queue\Exception\SerializationException;
use snipershady\Queue\Exception\UnknownChannelException;
use snipershady\Queue\Exception\UnrecoverableMessageException;
use snipershady\Queue\Message\Delivery;
use snipershady\Queue\Message\FailureKind;
use snipershady\Queue\Message\FailureReason;
use snipershady\Queue\Message\Message;
use snipershady\Queue\Middleware\TimeoutMiddleware;
use snipershady\Queue\System\MemoryMeterInterface;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\SignalHandlerInterface;
use snipershady\Queue\System\SystemClock;
use snipershady\Queue\Text\TextDecoder;
use snipershady\Queue\Transport\Capability\BatchAckAwareInterface;
use snipershady\Queue\Transport\Capability\MaintenanceAwareInterface;
use snipershady\Queue\Transport\OrphanRecovery;
use snipershady\Queue\Transport\TransportInterface;
use snipershady\Queue\Worker\Event\MessageDeadLettered;
use snipershady\Queue\Worker\Event\MessageDiscarded;
use snipershady\Queue\Worker\Event\MessageFailed;
use snipershady\Queue\Worker\Event\MessageHandled;
use snipershady\Queue\Worker\Event\MessageReceived;
use snipershady\Queue\Worker\Event\MessageReleased;
use snipershady\Queue\Worker\Event\MessageRetried;
use snipershady\Queue\Worker\Event\WorkerEventInterface;
use snipershady\Queue\Worker\Event\WorkerStopped;
use Throwable;

/**
 * Consuma una coda: per ogni messaggio applica controlli (consegne, scadenza, canale), middleware e handler,
 * poi conferma, ritenta o sposta in DLQ.
 *
 * @api
 */
final class Worker implements WorkerInterface
{
    private readonly HandlerRegistry $handlers;

    private readonly StopConditions $stopConditions;

    private bool $stopRequested = false;

    private ?int $lastMaintenanceMs = null;

    private int $sinceLastGc = 0;

    /** @var list<Delivery> */
    private array $pendingAcks = [];

    /**
     * Età massima (ms dalla ricezione) per iniziare a elaborare una consegna; null se il backend non riprende
     * gli orfani dopo `claimIdleMs` (OrphanRecovery::Native).
     */
    private readonly ?int $startDeadlineMs;

    /**
     * Con un backend che riprende gli orfani dopo `claimIdleMs` (Redis, InMemory), tutto il batch ricevuto
     * risulta "in elaborazione" da subito, anche i messaggi che aspettano il proprio turno. Per non farli
     * riprendere da un altro worker mentre sono ancora qui, una consegna viene elaborata solo se è stata
     * ricevuta da meno di `claimIdleMs - timeout dell'handler` (TimeoutMiddleware) oppure, senza timeout,
     * da meno di `claimIdleMs / 2`; le altre vengono restituite con `release()`.
     *
     * Complessità: O(m), m = numero di middleware.
     *
     * @throws InvalidArgumentException se il timeout dell'handler non è inferiore a `QueueConfig::$claimIdleMs`
     */
    public function __construct(
        private readonly TransportInterface $transport,
        private readonly QueueConfig $queueConfig = new QueueConfig(),
        private readonly WorkerConfig $config = new WorkerConfig(),
        private readonly ClockInterface $clock = new SystemClock(),
        private readonly SignalHandlerInterface $signalHandler = new NullSignalHandler(),
        private readonly MemoryMeterInterface $memoryMeter = new NativeMemoryMeter(),
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly TextDecoder $textDecoder = new TextDecoder(),
    ) {
        $timeoutsMs = [];
        foreach ($config->middlewares as $middleware) {
            if (!$middleware instanceof TimeoutMiddleware) {
                continue;
            }

            // Un handler ancora in esecuzione oltre claimIdleMs verrebbe ripreso da un altro worker.
            if ($middleware->timeoutSeconds() * 1000 >= $queueConfig->claimIdleMs) {
                throw new InvalidArgumentException(\sprintf('Handler timeout (%d s) must be shorter than claimIdleMs (%d ms).', $middleware->timeoutSeconds(), $queueConfig->claimIdleMs));
            }

            $timeoutsMs[] = $middleware->timeoutSeconds() * 1000;
        }

        $handlerBudgetMs = [] === $timeoutsMs ? intdiv($queueConfig->claimIdleMs, 2) : \max($timeoutsMs);
        $this->startDeadlineMs = OrphanRecovery::Explicit === $transport->capabilities()->orphanRecovery
            ? $queueConfig->claimIdleMs - $handlerBudgetMs
            : null;

        $this->handlers = new HandlerRegistry();
        $this->stopConditions = new StopConditions($config, $clock, $memoryMeter);
    }

    /**
     * Complessità: O(c), c = lunghezza del canale (validazione).
     */
    #[Override]
    public function on(string $channel, HandlerInterface|callable $handler): static
    {
        $this->handlers->register($channel, $handler);

        return $this;
    }

    /**
     * Complessità: O(1).
     */
    #[Override]
    public function stop(): void
    {
        $this->stopRequested = true;
    }

    /**
     * Complessità: O(1) per messaggio più handler, middleware (m) e listener (l): O(m + l) di overhead; dura finché un
     * limite o un segnale non lo ferma.
     */
    #[Override]
    public function run(): WorkerExitReason
    {
        $this->stopRequested = false;
        $this->signalHandler->onTerminate(function (int $signal): void {
            $this->logger->notice('Received signal {signal}: stopping after the current message.', ['signal' => $signal]);
            $this->stop();
        });

        $startedAt = $this->clock->now();
        $processed = 0;

        try {
            while (true) {
                $reason = $this->stopRequested
                    ? WorkerExitReason::StopRequested
                    : $this->stopConditions->check($processed, $startedAt);
                if ($reason instanceof WorkerExitReason) {
                    break;
                }

                [$handled, $received] = $this->cycle($this->stopConditions->nextBatchSize($processed));
                $processed += $handled;

                if (0 === $received && $this->config->stopWhenEmpty) {
                    $reason = WorkerExitReason::QueueEmpty;

                    break;
                }
            }
        } finally {
            $this->signalHandler->reset();
        }

        $this->logger->info('Worker stopped ({reason}) after {processed} messages.', ['reason' => $reason->value, 'processed' => $processed]);
        $this->emit(new WorkerStopped($reason, $processed));

        return $reason;
    }

    /**
     * Complessità: O(k·(m + l)) più gli handler, k = consegne ricevute (al più batchSize), m = middleware, l =
     * listener; una `receive()` e al più una `ackBatch()`.
     */
    #[Override]
    public function runOnce(): int
    {
        return $this->cycle($this->config->batchSize)[0];
    }

    /**
     * @return array{int, int} messaggi elaborati e messaggi ricevuti
     */
    private function cycle(int $max): array
    {
        $this->maintainIfDue();

        $deliveries = $this->transport->receive($max, $this->config->receiveTimeoutMs);
        $handled = 0;
        $expired = 0;

        foreach ($deliveries as $delivery) {
            if ($this->stopRequested) {
                $this->release($delivery);

                continue;
            }

            // Iniziarla ora rischierebbe di superare claimIdleMs: un altro worker la riprenderebbe in parallelo.
            if (null !== $this->startDeadlineMs && $this->elapsedMs($delivery->receivedAt) >= $this->startDeadlineMs) {
                $this->release($delivery);
                ++$expired;

                continue;
            }

            $this->process($delivery);
            ++$handled;
        }

        if ($expired > 0) {
            $this->logger->warning('{released} messages released unprocessed: the batch took longer than {deadline_ms} ms, the safe window before claimIdleMs. Lower batchSize or the handler timeout.', [
                'released' => $expired,
                'deadline_ms' => $this->startDeadlineMs,
            ]);
        }

        $this->flushAcks();
        $this->collectGarbageIfDue($handled);

        return [$handled, \count($deliveries)];
    }

    private function process(Delivery $delivery): void
    {
        $this->emit(new MessageReceived($delivery));
        $message = $delivery->message;
        $now = $this->clock->now();

        if ($this->transport->capabilities()->deliveryCount && $delivery->deliveryCount > $this->queueConfig->maxDeliveries) {
            $this->deadLetter($delivery, new FailureReason(
                FailureKind::MaxDeliveriesExceeded,
                $message->id,
                $now,
                \sprintf('Delivered %d times without acknowledgement (max %d).', $delivery->deliveryCount, $this->queueConfig->maxDeliveries),
            ));

            return;
        }

        if ($message->isExpired($now)) {
            $this->expired($delivery);

            return;
        }

        $handler = $this->handlers->resolve($message->channel);
        if (!$handler instanceof HandlerInterface) {
            $this->unknownChannel($delivery);

            return;
        }

        try {
            $this->runPipeline($message, $handler);
        } catch (UnrecoverableMessageException $unrecoverableMessageException) {
            $this->failed($delivery, $unrecoverableMessageException, FailureKind::Unrecoverable);

            return;
        } catch (SerializationException $serializationException) {
            $this->failed($delivery, $serializationException, FailureKind::Serialization);

            return;
        } catch (Throwable $throwable) {
            $this->failed($delivery, $throwable, FailureKind::HandlerFailed);

            return;
        }

        // Fuori dal try: un errore del backend nell'ack non è un fallimento dell'handler da ritentare.
        $this->ack($delivery);
        $this->emit(new MessageHandled($delivery, $this->elapsedMs($now)));
    }

    private function runPipeline(Message $message, HandlerInterface $handler): void
    {
        $next = function (Message $message) use ($handler): void {
            $handler->handle($message, $this->textDecoder->decode($message->text));
        };

        foreach (array_reverse($this->config->middlewares) as $middleware) {
            $next = static function (Message $message) use ($middleware, $next): void {
                $middleware->process($message, $next);
            };
        }

        $next($message);
    }

    private function failed(Delivery $delivery, Throwable $throwable, FailureKind $failureKind): void
    {
        $this->emit(new MessageFailed($delivery, $throwable));
        $message = $delivery->message;
        $attempt = $message->attempts + 1;

        $retryable = FailureKind::HandlerFailed === $failureKind
            && ($throwable instanceof RetryableException || $this->config->retryAnyThrowable);

        if ($retryable && $attempt < $this->queueConfig->maxAttempts) {
            $delayMs = $throwable instanceof RetryableException && null !== $throwable->delayMs
                ? $throwable->delayMs
                : $this->config->retryStrategy->nextDelayMs($attempt);

            if (null !== $delayMs) {
                $this->transport->retry($delivery, $delayMs);
                $this->logger->warning('Message {message_id} failed (attempt {attempt}/{max_attempts}), retrying in {delay_ms} ms: {error}', [
                    'message_id' => $message->id->value,
                    'attempt' => $attempt,
                    'max_attempts' => $this->queueConfig->maxAttempts,
                    'delay_ms' => $delayMs,
                    'error' => $throwable->getMessage(),
                ]);
                $this->emit(new MessageRetried($delivery, $delayMs));

                return;
            }
        }

        $this->deadLetter($delivery, FailureReason::fromThrowable($failureKind, $message->id, $this->clock->now(), $throwable));
    }

    private function expired(Delivery $delivery): void
    {
        if (ExpiredMessagePolicy::Discard === $this->config->expiredMessagePolicy) {
            $this->discard($delivery, FailureKind::Expired);

            return;
        }

        $this->deadLetter($delivery, new FailureReason(FailureKind::Expired, $delivery->message->id, $this->clock->now(), 'Message expired before processing.'));
    }

    private function unknownChannel(Delivery $delivery): void
    {
        if (UnknownChannelPolicy::Discard === $this->config->unknownChannelPolicy) {
            $this->discard($delivery, FailureKind::UnknownChannel);

            return;
        }

        $this->deadLetter($delivery, FailureReason::fromThrowable(
            FailureKind::UnknownChannel,
            $delivery->message->id,
            $this->clock->now(),
            UnknownChannelException::forChannel($delivery->message->channel),
        ));
    }

    private function deadLetter(Delivery $delivery, FailureReason $failureReason): void
    {
        $this->transport->reject($delivery, $failureReason);
        $this->logger->error('Message {message_id} moved to the dead letter queue ({kind}): {error}', [
            'message_id' => $delivery->message->id->value,
            'kind' => $failureReason->kind->value,
            'error' => $failureReason->errorMessage,
        ]);
        $this->emit(new MessageDeadLettered($delivery, $failureReason));
    }

    private function discard(Delivery $delivery, FailureKind $failureKind): void
    {
        $this->ack($delivery);
        $this->logger->warning('Message {message_id} discarded ({reason}).', ['message_id' => $delivery->message->id->value, 'reason' => $failureKind->value]);
        $this->emit(new MessageDiscarded($delivery, $failureKind));
    }

    private function release(Delivery $delivery): void
    {
        $this->flushAcks();
        $this->transport->release($delivery);
        $this->emit(new MessageReleased($delivery));
    }

    private function ack(Delivery $delivery): void
    {
        if ($this->transport instanceof BatchAckAwareInterface && $this->transport->capabilities()->batchAck) {
            $this->pendingAcks[] = $delivery;

            return;
        }

        $this->transport->ack($delivery);
    }

    private function flushAcks(): void
    {
        if ([] === $this->pendingAcks || !$this->transport instanceof BatchAckAwareInterface) {
            return;
        }

        $pending = $this->pendingAcks;
        $this->pendingAcks = [];
        $this->transport->ackBatch($pending);
    }

    private function maintainIfDue(): void
    {
        if (!$this->transport instanceof MaintenanceAwareInterface) {
            return;
        }

        $nowMs = (int) $this->clock->now()->format('Uv');
        if (null !== $this->lastMaintenanceMs && $nowMs - $this->lastMaintenanceMs < $this->config->maintenanceIntervalMs) {
            return;
        }

        $this->lastMaintenanceMs = $nowMs;
        $this->transport->maintain();
    }

    private function collectGarbageIfDue(int $handled): void
    {
        if (0 === $this->config->gcEveryMessages) {
            return;
        }

        $this->sinceLastGc += $handled;
        if ($this->sinceLastGc >= $this->config->gcEveryMessages) {
            $this->sinceLastGc = 0;
            $this->memoryMeter->collectCycles();
        }
    }

    private function emit(WorkerEventInterface $workerEvent): void
    {
        foreach ($this->config->listeners as $listener) {
            try {
                $listener->onEvent($workerEvent);
            } catch (Throwable $throwable) {
                $this->logger->error('Worker event listener {listener} failed: {error}', [
                    'listener' => $listener::class,
                    'error' => $throwable->getMessage(),
                    'exception' => $throwable,
                ]);
            }
        }
    }

    private function elapsedMs(DateTimeImmutable $since): int
    {
        return (int) $this->clock->now()->format('Uv') - (int) $since->format('Uv');
    }
}
