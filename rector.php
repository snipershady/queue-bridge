<?php

declare(strict_types=1);

use Rector\CodeQuality\Rector\Attribute\ExplicitAttributeNamedArgsRector;
use Rector\Config\RectorConfig;
use Rector\PHPUnit\CodeQuality\Rector\Class_\PreferPHPUnitThisCallRector;
use Rector\TypeDeclaration\Rector\ClassMethod\AddParamArrayDocblockBasedOnCallableNativeFuncCallRector;
use Rector\TypeDeclarationDocblocks\Rector\ClassMethod\AddReturnDocblockForArrayDimAssignedObjectRector;

return RectorConfig::configure()
                ->withPaths([
                    __DIR__.'/src',
                    __DIR__.'/tests',
                    __DIR__.'/bin',
                    __DIR__.'/tools',
                    __DIR__.'/examples',
                ])
                ->withParallel()
                // Nessun argomento: target = versione minima di composer.json (8.3).
                // Mai set php84+ o LevelSetList::UP_TO_PHP_84+: produrrebbero codice non eseguibile su 8.3.
                ->withPhpSets()
                ->withPreparedSets(
                    deadCode: true,
                    codeQuality: true,
                    codingStyle: true,
                    typeDeclarations: true,
                    typeDeclarationDocblocks: true,
                    privatization: true,
                    // naming disattivato: le regole Rename*ToMatch* rinominano parametri e proprietà promosse,
                    // che con i named arguments fanno parte delle firme pubbliche (CLAUDE.md §5).
                    namedArgs: true,
                    instanceOf: true,
                    earlyReturn: true,
                    phpunitCodeQuality: true,
                    phpunitNarrowAsserts: true,
                    phpunitMockToStub: true,
                )
                ->withSkip([
                    // Conflitto con php-cs-fixer php_unit_test_case_static_method_calls (call_type: self),
                    // configurato esplicitamente: vince la regola più specifica, quindi la si salta qui.
                    PreferPHPUnitThisCallRector::class,
                    // Gli attributi PHPUnit sono @no-named-arguments (PHPStan argument.named): niente named args nei test.
                    ExplicitAttributeNamedArgsRector::class => [__DIR__.'/tests'],
                    // Aggiungono docblock più generici dei tipi ereditati dall'interfaccia (es. mixed[][] al posto di
                    // non-empty-list<non-empty-list<string>>), degradando le firme verificate da PHPStan.
                    AddParamArrayDocblockBasedOnCallableNativeFuncCallRector::class,
                    AddReturnDocblockForArrayDimAssignedObjectRector::class,
                ])
                ->withCache(__DIR__.'/.rector.cache')
                ->withAttributesSets(phpunit: true)
                ->withComposerBased(phpunit: true)
                ->withImportNames(
                    importShortClasses: false,
                    removeUnusedImports: true,
                )
                ->withPHPStanConfigs([__DIR__.'/phpstan.neon'])
;
