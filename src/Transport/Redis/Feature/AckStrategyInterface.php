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
     */
    public function mode(): string;
}
