<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Support;

/**
 * Registro delle chiamate, per verificare l'ordine di esecuzione di middleware e handler.
 */
final class CallLog
{
    /** @var list<string> */
    public array $calls = [];

    public function record(string $call): void
    {
        $this->calls[] = $call;
    }
}
