<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\InMemory;

use snipershady\Queue\Message\MessageId;
use snipershady\Queue\Transport\DeadLetter;

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

    public function enqueue(InMemoryEntry $inMemoryEntry, int $dueMs, int $nowMs): void
    {
        if ($dueMs <= $nowMs) {
            $this->ready[] = $inMemoryEntry;

            return;
        }

        $this->delayed[] = ['dueMs' => $dueMs, 'entry' => $inMemoryEntry];
    }

    public function pushFront(InMemoryEntry $inMemoryEntry): void
    {
        array_unshift($this->ready, $inMemoryEntry);
    }

    /**
     * Sposta in coda i differiti scaduti, in ordine di scadenza (a parità, di inserimento).
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
     */
    public function settle(string $receipt): ?InMemoryEntry
    {
        $inFlight = $this->inFlight[$receipt] ?? null;
        unset($this->inFlight[$receipt]);

        return $inFlight['entry'] ?? null;
    }

    /**
     * Rimette in coda i messaggi in elaborazione da almeno $idleMs: la vecchia ricevuta smette di valere.
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

    public function addDeadLetter(DeadLetter $deadLetter, int $maxLength): void
    {
        $this->deadLetters[] = $deadLetter;
        if (\count($this->deadLetters) > $maxLength) {
            $this->deadLetters = \array_slice($this->deadLetters, -$maxLength);
        }
    }

    /**
     * @return list<DeadLetter>
     */
    public function deadLetters(int $limit): array
    {
        return \array_slice($this->deadLetters, 0, $limit);
    }

    /**
     * @return list<DeadLetter>
     */
    public function takeDeadLetters(int $limit): array
    {
        $taken = \array_slice($this->deadLetters, 0, $limit);
        $this->deadLetters = \array_slice($this->deadLetters, $limit);

        return $taken;
    }

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

    public function purgeDeadLetters(): int
    {
        $count = \count($this->deadLetters);
        $this->deadLetters = [];

        return $count;
    }

    public function readyCount(): int
    {
        return \count($this->ready);
    }

    /**
     * Messaggi pronti mai consegnati: l'equivalente del lag di un consumer group.
     */
    public function neverDeliveredCount(): int
    {
        return \count(array_filter($this->ready, static fn (InMemoryEntry $inMemoryEntry): bool => 0 === $inMemoryEntry->deliveries));
    }

    public function inFlightCount(): int
    {
        return \count($this->inFlight);
    }

    public function delayedCount(): int
    {
        return \count($this->delayed);
    }

    public function deadLetterCount(): int
    {
        return \count($this->deadLetters);
    }
}
