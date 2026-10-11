<?php

declare(strict_types=1);

namespace snipershady\Queue\Cli;

/**
 * Argomenti della riga di comando letti da `$_SERVER['argv']` tramite filter_var(): funziona con qualunque
 * valore di `register_argc_argv` nel php.ini, senza dipendere dalla variabile globale `$argv`.
 *
 * @api
 */
final class CommandLine
{
    /**
     * Complessità: O(a), a = numero di argomenti.
     *
     * @return list<string> argomenti dopo il nome dello script; lista vuota se non disponibili
     */
    public static function arguments(): array
    {
        $argv = filter_var($_SERVER['argv'] ?? null, \FILTER_UNSAFE_RAW, \FILTER_REQUIRE_ARRAY);
        if (!\is_array($argv)) {
            return [];
        }

        return array_values(array_filter(\array_slice(array_values($argv), 1), \is_string(...)));
    }
}
