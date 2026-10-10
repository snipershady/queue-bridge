<?php

declare(strict_types=1);

namespace snipershady\Queue\Text;

/**
 * @api
 */
enum TextKind: string
{
    case Json = 'json';
    case Plain = 'plain';
}
