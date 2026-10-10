<?php

declare(strict_types=1);

namespace snipershady\Queue\Factory;

use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use snipershady\Queue\Config\QueueConfig;
use snipershady\Queue\Exception\UnsupportedStrategyException;
use snipershady\Queue\Producer\ProducerMiddlewareInterface;
use snipershady\Queue\Queue;
use snipershady\Queue\QueueInterface;
use snipershady\Queue\System\MemoryMeterInterface;
use snipershady\Queue\System\NativeMemoryMeter;
use snipershady\Queue\System\NullSignalHandler;
use snipershady\Queue\System\SignalHandlerInterface;
use snipershady\Queue\System\SystemClock;

/**
 * Sceglie il builder che accetta il DTO (Chain of Responsibility) e costruisce la facade Queue. Non conosce
 * nessun backend: i builder forniti li registra QueueBridge::factory().
 *
 * @api
 */
final readonly class QueueFactory implements QueueFactoryInterface
{
    /**
     * @param list<TransportBuilderInterface>   $builders            consultati in ordine; il primo che supporta il DTO vince
     * @param list<ProducerMiddlewareInterface> $producerMiddlewares
     *
     * @throws InvalidArgumentException se non ci sono builder
     */
    public function __construct(
        private array $builders,
        private ClockInterface $clock = new SystemClock(),
        private LoggerInterface $logger = new NullLogger(),
        private SignalHandlerInterface $signalHandler = new NullSignalHandler(),
        private MemoryMeterInterface $memoryMeter = new NativeMemoryMeter(),
        private array $producerMiddlewares = [],
    ) {
        if ([] === $builders) {
            throw new InvalidArgumentException('At least one transport builder is required.');
        }
    }

    #[Override]
    public function create(StrategyDtoInterface $strategy, QueueConfig $config = new QueueConfig()): QueueInterface
    {
        foreach ($this->builders as $builder) {
            if ($builder->supports($strategy)) {
                $this->logger->debug('Creating queue {queue} on {strategy}.', ['queue' => $config->name, 'strategy' => $strategy->name()]);

                return new Queue(
                    $builder->build($strategy, $config),
                    $config,
                    $this->clock,
                    $this->logger,
                    $this->signalHandler,
                    $this->memoryMeter,
                    $this->producerMiddlewares,
                );
            }
        }

        throw UnsupportedStrategyException::noBuilderFor($strategy, array_map(static fn (TransportBuilderInterface $transportBuilder): string => $transportBuilder::class, $this->builders));
    }
}
