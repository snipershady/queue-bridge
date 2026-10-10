<?php

declare(strict_types=1);

use Rector\Config\RectorConfig;

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
                    naming: true,
                    namedArgs: true,
                    instanceOf: true,
                    earlyReturn: true,
                    phpunitCodeQuality: true,
                    phpunitNarrowAsserts: true,
                    phpunitMockToStub: true,
                )
                ->withCache(__DIR__.'/.rector.cache')
                ->withAttributesSets(phpunit: true)
                ->withComposerBased(phpunit: true)
                ->withImportNames(
                    importShortClasses: false,
                    removeUnusedImports: true,
                )
                ->withPHPStanConfigs([__DIR__.'/phpstan.neon'])
;
