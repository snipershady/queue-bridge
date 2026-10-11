<?php

declare(strict_types=1);

/*
 * Gate di coverage al 100%: metodi, istruzioni, branch ed elementi.
 *
 * Uso: php tools/check-coverage.php build/clover.xml
 *
 * Il clover deve essere generato con Xdebug e `--path-coverage`: senza, i branch (conditionals) valgono 0
 * e il controllo non verificherebbe nulla, quindi il gate fallisce.
 */

const METRICS = [
    'methods' => 'coveredmethods',
    'statements' => 'coveredstatements',
    'conditionals' => 'coveredconditionals',
    'elements' => 'coveredelements',
];

/**
 * @param list<string> $arguments
 */
function main(array $arguments): int
{
    $path = $arguments[0] ?? 'build/clover.xml';
    if (!is_file($path)) {
        fwrite(\STDERR, sprintf("Report clover non trovato: %s\n", $path));

        return 1;
    }

    $clover = simplexml_load_file($path);
    $metrics = $clover instanceof SimpleXMLElement ? first($clover->xpath('/coverage/project/metrics')) : null;
    if (!$clover instanceof SimpleXMLElement || !$metrics instanceof SimpleXMLElement) {
        fwrite(\STDERR, sprintf("Report clover non valido: %s\n", $path));

        return 1;
    }

    $counts = [];
    foreach (METRICS as $total => $covered) {
        $counts[$total] = [(int) $metrics[$total], (int) $metrics[$covered]];
    }

    if ($counts['methods'][0] > 0 && 0 === $counts['conditionals'][0]) {
        fwrite(\STDERR, "Il report non contiene la branch coverage: generalo con Xdebug e --path-coverage.\n");

        return 1;
    }

    $failed = false;
    foreach ($counts as $name => [$total, $covered]) {
        $ok = $covered === $total;
        $failed = $failed || !$ok;
        fwrite(\STDOUT, sprintf("%s %-12s %d/%d\n", $ok ? 'OK  ' : 'FAIL', $name, $covered, $total));
    }

    if ($failed) {
        report(uncovered($clover));

        return 1;
    }

    fwrite(\STDOUT, "Coverage 100%.\n");

    return 0;
}

/**
 * @param list<string> $lines
 */
function report(array $lines): void
{
    foreach ($lines as $line) {
        fwrite(\STDERR, $line."\n");
    }

    fwrite(\STDERR, "Branch scoperti su righe coperte: phpunit --path-coverage --coverage-html build/coverage\n");
}

/**
 * @param array<SimpleXMLElement>|false|null $nodes
 */
function first(array|false|null $nodes): ?SimpleXMLElement
{
    return is_array($nodes) && [] !== $nodes ? reset($nodes) : null;
}

/**
 * @param array<SimpleXMLElement>|false|null $nodes
 *
 * @return list<SimpleXMLElement>
 */
function all(array|false|null $nodes): array
{
    return is_array($nodes) ? array_values($nodes) : [];
}

/**
 * @return list<string>
 */
function uncovered(SimpleXMLElement $clover): array
{
    $result = [];
    foreach (all($clover->xpath('//file')) as $file) {
        $name = (string) $file['name'];
        foreach (all($file->xpath('line[@count="0"]')) as $line) {
            $result[] = 'method' === (string) $line['type']
                ? sprintf('  metodo scoperto     %s:%d %s()', $name, (int) $line['num'], (string) $line['name'])
                : sprintf('  istruzione scoperta %s:%d', $name, (int) $line['num']);
        }
    }

    return $result;
}

// $_SERVER['argv'] tramite filter_var(): non dipende da register_argc_argv nel php.ini.
$argv = filter_var($_SERVER['argv'] ?? null, \FILTER_UNSAFE_RAW, \FILTER_REQUIRE_ARRAY);

exit(main(is_array($argv) ? array_values(array_filter(array_slice(array_values($argv), 1), is_string(...))) : []));
