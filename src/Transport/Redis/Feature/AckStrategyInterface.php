<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Feature;

/**
 * Come si conferma ed elimina un'entry (dentro gli script Lua).
 *
 * @internal
 */
interface AckStrategyInterface
{
    /**
     * Modalità passata agli script di RedisScripts: `xackdel` oppure `xack_xdel`.
     *
     * Complessità: O(1).
     */
    public function mode(): string;
}
