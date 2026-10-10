<?php

declare(strict_types=1);

namespace snipershady\Queue\Tests\Architecture;

use PHPat\Selector\Selector;
use PHPat\Selector\SelectorInterface;
use PHPat\Test\Builder\Rule;
use PHPat\Test\PHPat;

/**
 * Regole di dipendenza di CLAUDE.md §3.1, verificate da PHPStan tramite phpat.
 */
final class LayerTest
{
    private const string ROOT = 'snipershady\Queue';

    private const string IN_MEMORY = self::ROOT.'\Transport\InMemory';

    private const string REDIS = self::ROOT.'\Transport\Redis';

    private const string RABBIT_MQ = self::ROOT.'\Transport\RabbitMq';

    private const string DSN = self::ROOT.'\Dsn';

    private const string CLI = self::ROOT.'\Cli';

    private const string COMPOSITION_ROOT = self::ROOT.'\QueueBridge';

    private const string TESTS = self::ROOT.'\Tests';

    public function testCoreDependsOnNoAdapterNorCompositionRoot(): Rule
    {
        return PHPat::rule()
            ->classes(Selector::inNamespace(self::ROOT))
            ->excluding(Selector::inNamespace(self::TESTS), ...$this->adapters(), ...$this->compositionRoot())
            ->shouldNot()->dependOn()
            ->classes(...$this->adapters(), ...$this->compositionRoot())
            ->because('il core dipende solo da interfacce: i backend concreti li conosce solo il composition root');
    }

    public function testInMemoryAdapterDependsOnNoOtherAdapter(): Rule
    {
        return $this->adapterIsolated(
            Selector::inNamespace(self::IN_MEMORY),
            Selector::inNamespace(self::REDIS),
            Selector::inNamespace(self::RABBIT_MQ),
        );
    }

    public function testRedisAdapterDependsOnNoOtherAdapter(): Rule
    {
        return $this->adapterIsolated(
            Selector::inNamespace(self::REDIS),
            Selector::inNamespace(self::IN_MEMORY),
            Selector::inNamespace(self::RABBIT_MQ),
        );
    }

    public function testRabbitMqAdapterDependsOnNoOtherAdapter(): Rule
    {
        return $this->adapterIsolated(
            Selector::inNamespace(self::RABBIT_MQ),
            Selector::inNamespace(self::IN_MEMORY),
            Selector::inNamespace(self::REDIS),
        );
    }

    private function adapterIsolated(SelectorInterface $adapter, SelectorInterface ...$others): Rule
    {
        return PHPat::rule()
            ->classes($adapter)
            ->shouldNot()->dependOn()
            ->classes(...$others, ...$this->compositionRoot())
            ->because('ogni backend è autocontenuto e può diventare un pacchetto separato');
    }

    /**
     * @return list<SelectorInterface>
     */
    private function adapters(): array
    {
        return [
            Selector::inNamespace(self::IN_MEMORY),
            Selector::inNamespace(self::REDIS),
            Selector::inNamespace(self::RABBIT_MQ),
        ];
    }

    /**
     * @return list<SelectorInterface>
     */
    private function compositionRoot(): array
    {
        return [
            Selector::inNamespace(self::DSN),
            Selector::inNamespace(self::CLI),
            Selector::classname(self::COMPOSITION_ROOT),
        ];
    }
}
