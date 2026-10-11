<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use snipershady\Queue\Transport\Redis\Version\RedisCapabilities;

/**
 * Le strategie scelte per una versione di Redis. RedisTransport le riceve già pronte e non conosce la versione.
 *
 * @internal
 */
final readonly class RedisFeatures
{
    /**
     * Complessità: O(1).
     */
    public function __construct(
        public RedisCapabilities $capabilities,
        public IdleClaimStrategyInterface $idleClaim,
        public AckStrategyInterface $ack,
        public TrimStrategyInterface $trim,
        public LagStrategyInterface $lag,
        public LockReleaseStrategyInterface $lockRelease,
    ) {
    }
}
