<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Version;

/**
 * Funzionalità per feature (non per versione): il codice chiede `$caps->xackdel`, mai `$version >= ...`.
 *
 * @internal
 */
final readonly class RedisCapabilities
{
    /**
     * @param bool $xpendingIdle   `XPENDING ... IDLE` (filtro di inattività lato server)
     * @param bool $xtrimMinId     `XTRIM ... MINID`
     * @param bool $groupLag       `XINFO GROUPS` con `lag`
     * @param bool $xackdel        `XACKDEL` (ack + delete atomico)
     * @param bool $trimAcked      opzione `ACKED` di XTRIM (rimuove solo entry confermate)
     * @param bool $readGroupClaim `XREADGROUP ... CLAIM` (recupero orfani in lettura)
     * @param bool $delex          `DELEX ... IFEQ` (cancellazione condizionata)
     */
    public function __construct(
        public RedisVersionEnum $version,
        public bool $xpendingIdle,
        public bool $xtrimMinId,
        public bool $groupLag,
        public bool $xackdel,
        public bool $trimAcked,
        public bool $readGroupClaim,
        public bool $delex,
    ) {
    }

    public static function forVersion(RedisVersionEnum $redisVersionEnum): self
    {
        return new self(
            version: $redisVersionEnum,
            xpendingIdle: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE62),
            xtrimMinId: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE62),
            groupLag: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE70),
            xackdel: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE82),
            trimAcked: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE82),
            readGroupClaim: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE84),
            delex: $redisVersionEnum->isAtLeast(RedisVersionEnum::GE84),
        );
    }
}
