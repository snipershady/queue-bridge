<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

use Override;

/**
 * Redis >= 8.2: XACKDEL, ack e delete con un solo comando.
 *
 * @internal
 */
final readonly class AckDelStrategy implements AckStrategyInterface
{
    /**
     * Complessità: O(1).
     */
    #[Override]
    public function mode(): string
    {
        return 'xackdel';
    }
}
