<?php

declare(strict_types=1);

namespace QueueBridge\Transport\Redis\Feature;

use Override;

/**
 * Redis >= 6.0: XACK + XDEL nello stesso script (atomici).
 *
 * @internal
 */
final readonly class AckThenDeleteStrategy implements AckStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function mode(): string
    {
        return 'xack_xdel';
    }
}
