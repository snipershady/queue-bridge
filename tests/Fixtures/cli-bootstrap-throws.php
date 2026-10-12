<?php

declare(strict_types=1);

/*
 * Bootstrap che fallisce mentre viene caricato: la CLI deve uscire con l'exit code d'uso, non con un fatal.
 */
throw new RuntimeException('cannot load the container');
