<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Version;

use snipershady\Queue\Exception\VersionMismatchException;

/**
 * Livelli di Redis che cambiano il modo in cui la libreria esegue i comandi (CLAUDE.md §7.3).
 *
 * @api
 */
enum RedisVersionEnum: string
{
    case GE60 = '6.0';
    case GE62 = '6.2';
    case GE70 = '7.0';
    case GE80 = '8.0';
    case GE82 = '8.2';
    case GE84 = '8.4';

    /**
     * Livello più alto <= alla versione del server (es. "7.4.2" → GE70).
     *
     * @throws VersionMismatchException se la versione è sotto 6.0 o non è riconoscibile
     */
    public static function fromServerVersion(string $version): self
    {
        if (1 !== preg_match('/^(\d+)\.(\d+)/', $version, $matches)) {
            throw VersionMismatchException::unsupported($version, self::GE60->value);
        }

        $server = [(int) $matches[1], (int) $matches[2]];
        $level = null;
        foreach (self::cases() as $case) {
            if ($case->parts() <= $server) {
                $level = $case;
            }
        }

        return $level ?? throw VersionMismatchException::unsupported($version, self::GE60->value);
    }

    public function isAtLeast(self $other): bool
    {
        return $this->parts() >= $other->parts();
    }

    /**
     * @return array{int, int}
     */
    private function parts(): array
    {
        [$major, $minor] = explode('.', $this->value);

        return [(int) $major, (int) $minor];
    }
}
