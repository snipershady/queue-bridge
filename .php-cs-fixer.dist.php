<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;
use PhpCsFixer\Runner\Parallel\ParallelConfigFactory;

$finder = (new Finder())
        ->in([
            __DIR__.'/src',
            __DIR__.'/tests',
            __DIR__.'/bin',
            __DIR__.'/tools',
            __DIR__.'/examples',
        ])
        ->append([__FILE__, __DIR__.'/rector.php']);

return (new Config())
                ->setFinder($finder)
                ->setRiskyAllowed(true)
                ->setParallelConfig(ParallelConfigFactory::detect())
                ->setRules([
                    '@Symfony' => true,
                    '@Symfony:risky' => true,
                    // Target = versione minima (8.3). Non usare @PHP8x4Migration+: new_expression_parentheses richiede 8.4.
                    '@PHP8x3Migration' => true,
                    '@PHP80Migration:risky' => true,
                    '@PHPUnit100Migration:risky' => true,
                    // Regole dei set 8.4/8.5 che producono codice valido anche su 8.3 (prevengono deprecazioni).
                    'nullable_type_declaration_for_default_null_value' => true,
                    'switch_case_semicolon_to_colon' => true,
                    'declare_strict_types' => true,
                    'strict_param' => true,
                    'strict_comparison' => true,
                    'final_class' => true,
                    'final_internal_class' => true,
                    'static_lambda' => true,
                    'self_static_accessor' => true,
                    'void_return' => true,
                    'date_time_immutable' => true,
                    // Disattivata: i limiti di dimensione sono in byte (strlen), mb_strlen li trasformerebbe in caratteri.
                    'mb_str_functions' => false,
                    'use_arrow_functions' => true,
                    'native_function_invocation' => ['include' => ['@compiler_optimized'], 'scope' => 'namespaced', 'strict' => true],
                    'native_constant_invocation' => ['scope' => 'namespaced', 'strict' => true],
                    // import_functions/import_constants disattivati: entrerebbero in conflitto con native_*_invocation (\strlen).
                    'global_namespace_import' => ['import_classes' => true, 'import_constants' => false, 'import_functions' => false],
                    'ordered_imports' => ['imports_order' => ['class', 'function', 'const'], 'sort_algorithm' => 'alpha'],
                    'ordered_class_elements' => true,
                    'phpdoc_order' => true,
                    'phpdoc_line_span' => ['const' => 'single', 'property' => 'single', 'method' => 'multi'],
                    'php_unit_strict' => true,
                    'php_unit_test_case_static_method_calls' => ['call_type' => 'self'],
                    'no_superfluous_phpdoc_tags' => ['allow_mixed' => true, 'remove_inheritdoc' => true],
                    'trailing_comma_in_multiline' => ['elements' => ['arguments', 'arrays', 'match', 'parameters']],
                ]);
