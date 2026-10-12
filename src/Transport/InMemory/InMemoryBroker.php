<?php

declare(strict_types=1);

namespace QueueBridge\Transport\InMemory;

use DateTimeImmutable;
use QueueBridge\Message\MessageId;
use QueueBridge\Transport\DeadLetter;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueuedMessageState;

/**
 * Stato condiviso di una coda in memoria: l'equivalente del server Redis o RabbitMQ.
 * Più InMemoryTransport sullo stesso broker sono consumer concorrenti della stessa coda.
 *
 * @internal
 */
final class InMemoryBroker
{
    /** @var list<InMemoryEntry> */
    private array $ready = [];

    /** @var list<array{dueMs: int, entry: InMemoryEntry}> */
    private array $delayed = [];

    /** @var array<string, array{entry: InMemoryEntry, receivedAtMs: int}> */
    private array $inFlight = [];

    /** @var list<DeadLetter> */
    private array $deadLetters = [];

    private int $sequence = 0;

    /**
     * Complessità: O(1) ammortizzato.
     */
    public function enqueue(InMemoryEntry $inMemoryEntry, int $dueMs, int $nowMs): void
    {
        if ($dueMs <= $nowMs) {
            $this->ready[] = $inMemoryEntry;

            return;
        }

        $this->delayed[] = ['dueMs' => $dueMs, 'entry' => $inMemoryEntry];
    }

    /**
     * Rimette in fondo ai pronti, come XADD di una nuova copia su Redis.
     *
     * Complessità: O(1) ammortizzato.
     */
    public function pushBack(InMemoryEntry $inMemoryEntry): void
    {
        $this->ready[] = $inMemoryEntry;
    }

    /**
     * Sposta in coda i differiti scaduti, in ordine di scadenza (a parità, di inserimento).
     *
     * Complessità: O(d log d), d = differiti.
     */
    public function promoteDue(int $nowMs): int
    {
        $due = array_values(array_filter($this->delayed, static fn (array $item): bool => $item['dueMs'] <= $nowMs));
        if ([] === $due) {
            return 0;
        }

        $this->delayed = array_values(array_filter($this->delayed, static fn (array $item): bool => $item['dueMs'] > $nowMs));
        usort($due, static fn (array $a, array $b): int => $a['dueMs'] <=> $b['dueMs']);
        foreach ($due as $item) {
            $this->ready[] = $item['entry'];
        }

        return \count($due);
    }

    /**
     * Consegna fino a $max messaggi, registrandoli come in elaborazione con una ricevuta nuova.
     *
     * Complessità: O(k·r), k = messaggi presi, r = messaggi pronti (array_shift reindicizza).
     *
     * @return list<array{receipt: non-empty-string, entry: InMemoryEntry}>
     */
    public function take(int $max, int $nowMs): array
    {
        $taken = [];
        while (\count($taken) < $max && [] !== $this->ready) {
            $inMemoryEntry = array_shift($this->ready)->delivered();
            $receipt = 'mem-'.++$this->sequence;
            $this->inFlight[$receipt] = ['entry' => $inMemoryEntry, 'receivedAtMs' => $nowMs];
            $taken[] = ['receipt' => $receipt, 'entry' => $inMemoryEntry];
        }

        return $taken;
    }

    /**
     * Rimuove il messaggio dagli "in elaborazione"; null se la ricevuta non è più valida.
     *
     * Complessità: O(1).
     */
    public function settle(string $receipt): ?InMemoryEntry
    {
        $inFlight = $this->inFlight[$receipt] ?? null;
        unset($this->inFlight[$receipt]);

        return $inFlight['entry'] ?? null;
    }

    /**
     * Rimette in coda i messaggi in elaborazione da almeno $idleMs: la vecchia ricevuta smette di valere.
     *
     * Complessità: O(f), f = messaggi in elaborazione.
     */
    public function recoverOrphans(int $nowMs, int $idleMs): int
    {
        $recovered = 0;
        foreach ($this->inFlight as $receipt => $inFlight) {
            if ($nowMs - $inFlight['receivedAtMs'] < $idleMs) {
                continue;
            }

            unset($this->inFlight[$receipt]);
            $this->ready[] = $inFlight['entry'];
            ++$recovered;
        }

        return $recovered;
    }

    /**
     * Complessità: O(1) ammortizzato; O(l) quando la DLQ supera la lunghezza massima, l = messaggi in DLQ.
     */
    public function addDeadLetter(DeadLetter $deadLetter, int $maxLength): void
    {
        $this->deadLetters[] = $deadLetter;
        if (\count($this->deadLetters) > $maxLength) {
            $this->deadLetters = \array_slice($this->deadLetters, -$maxLength);
        }
    }

    /**
     * Complessità: O(limit).
     *
     * @return list<DeadLetter>
     */
    public function deadLetters(int $limit): array
    {
        return \array_slice($this->deadLetters, 0, $limit);
    }

    /**
     * Complessità: O(l), l = messaggi in DLQ.
     *
     * @return list<DeadLetter>
     */
    public function takeDeadLetters(int $limit): array
    {
        $taken = \array_slice($this->deadLetters, 0, $limit);
        $this->deadLetters = \array_slice($this->deadLetters, $limit);

        return $taken;
    }

    /**
     * Complessità: O(l), l = messaggi in DLQ.
     */
    public function takeDeadLetter(MessageId $messageId): ?DeadLetter
    {
        foreach ($this->deadLetters as $index => $deadLetter) {
            if ($deadLetter->message->id->equals($messageId)) {
                array_splice($this->deadLetters, $index, 1);

                return $deadLetter;
            }
        }

        return null;
    }

    /**
     * Complessità: O(1).
     */
    public function purgeDeadLetters(): int
    {
        $count = \count($this->deadLetters);
        $this->deadLetters = [];

        return $count;
    }

    /**
     * Rimuove pronti e differiti; gli "in elaborazione" restano.
     *
     * Complessità: O(1).
     */
    public function purge(): int
    {
        $count = \count($this->ready) + \count($this->delayed);
        $this->ready = [];
        $this->delayed = [];

        return $count;
    }

    /**
     * In elaborazione (in ordine di consegna), pronti, poi differiti per scadenza.
     *
     * Complessità: O(f + r + d log d), f = in elaborazione, r = pronti, d = differiti.
     *
     * @return list<QueuedMessage>
     */
    public function browse(int $limit): array
    {
        $delayed = $this->delayed;
        usort($delayed, static fn (array $a, array $b): int => $a['dueMs'] <=> $b['dueMs']);

        $queued = [];
        foreach ($this->inFlight as $inFlight) {
            $queued[] = new QueuedMessage($inFlight['entry']->message, QueuedMessageState::InFlight);
        }

        foreach ($this->ready as $inMemoryEntry) {
            $queued[] = new QueuedMessage($inMemoryEntry->message, QueuedMessageState::Ready);
        }

        foreach ($delayed as $item) {
            $availableAt = new DateTimeImmutable(\sprintf('@%d.%03d', intdiv($item['dueMs'], 1000), $item['dueMs'] % 1000));
            $queued[] = new QueuedMessage($item['entry']->message, QueuedMessageState::Delayed, $availableAt);
        }

        return \array_slice($queued, 0, $limit);
    }

    /**
     * Complessità: O(1).
     */
    public function readyCount(): int
    {
        return \count($this->ready);
    }

    /**
     * Complessità: O(1).
     */
    public function inFlightCount(): int
    {
        return \count($this->inFlight);
    }

    /**
     * Complessità: O(1).
     */
    public function delayedCount(): int
    {
        return \count($this->delayed);
    }

    /**
     * Complessità: O(1).
     */
    public function deadLetterCount(): int
    {
        return \count($this->deadLetters);
    }
}
