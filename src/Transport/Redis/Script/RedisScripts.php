<?php

declare(strict_types=1);

namespace snipershady\Queue\Transport\Redis\Script;

/**
 * Script Lua come costanti (nessun I/O). Tutte le chiavi di uno script appartengono alla stessa coda e
 * quindi allo stesso hash tag. Le funzioni del preludio controllano che il consumer possieda ancora la
 * consegna nella PEL: un'operazione su una consegna ripresa da un altro consumer non ha effetto.
 *
 * Modalità di ack (AckStrategyInterface): `xackdel` (Redis >= 8.2) oppure `xack_xdel`.
 *
 * @internal
 */
final class RedisScripts
{
    private const string PRELUDE = <<<'LUA'
        local function owned(stream, group, consumer, id)
          local pending = redis.call('XPENDING', stream, group, id, id, 1)
          return pending[1] ~= nil and pending[1][2] == consumer
        end
        local function ack(stream, group, mode, ids)
          if #ids == 0 then return end
          if mode == 'xackdel' then
            redis.call('XACKDEL', stream, group, 'IDS', #ids, unpack(ids))
          else
            redis.call('XACK', stream, group, unpack(ids))
            redis.call('XDEL', stream, unpack(ids))
          end
        end

        LUA;

    /**
     * KEYS: stream. ARGV: group, consumer, modalità di ack, ID... → numero di consegne confermate.
     */
    public static function ack(): string
    {
        return self::PRELUDE.<<<'LUA'
            local owned_ids = {}
            for i = 4, #ARGV do
              if owned(KEYS[1], ARGV[1], ARGV[2], ARGV[i]) then owned_ids[#owned_ids + 1] = ARGV[i] end
            end
            ack(KEYS[1], ARGV[1], ARGV[3], owned_ids)
            return #owned_ids
            LUA;
    }

    /**
     * Retry, reject e release: scrive la nuova copia e conferma l'originale, atomicamente.
     *
     * KEYS: stream, delayed, dlq. ARGV: group, consumer, modalità di ack, ID, destinazione
     * (`stream` | `delayed` | `dlq`), score del differito o lunghezza massima della DLQ, membro del
     * differito, campi... → 1 se eseguito, 0 se la consegna non è più posseduta.
     */
    public static function settle(): string
    {
        return self::PRELUDE.<<<'LUA'
            if not owned(KEYS[1], ARGV[1], ARGV[2], ARGV[4]) then return 0 end
            if ARGV[5] == 'delayed' then
              redis.call('ZADD', KEYS[2], ARGV[6], ARGV[7])
            else
              local fields = {}
              for i = 8, #ARGV do fields[#fields + 1] = ARGV[i] end
              if ARGV[5] == 'dlq' then
                redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[6], '*', unpack(fields))
              else
                redis.call('XADD', KEYS[1], '*', unpack(fields))
              end
            end
            ack(KEYS[1], ARGV[1], ARGV[3], {ARGV[4]})
            return 1
            LUA;
    }

    /**
     * Rimette in coda gli orfani: copia l'entry con il totale delle consegne nel campo `deliveries`, poi
     * conferma ed elimina l'originale. L'inattività viene ricontrollata qui, così due worker che fanno
     * manutenzione insieme non duplicano il messaggio.
     *
     * KEYS: stream. ARGV: group, modalità di ack, inattività minima in ms, ID... → numero di messaggi rimessi in coda.
     */
    public static function requeueOrphans(): string
    {
        return self::PRELUDE.<<<'LUA'
            local requeued = 0
            for i = 4, #ARGV do
              local id = ARGV[i]
              local pending = redis.call('XPENDING', KEYS[1], ARGV[1], id, id, 1)
              if pending[1] ~= nil and tonumber(pending[1][3]) >= tonumber(ARGV[3]) then
                local entry = redis.call('XRANGE', KEYS[1], id, id)
                if entry[1] ~= nil and entry[1][2] then
                  local deliveries = tonumber(pending[1][4])
                  local fields = {}
                  local source = entry[1][2]
                  for j = 1, #source, 2 do
                    if source[j] == 'deliveries' then
                      deliveries = deliveries + tonumber(source[j + 1])
                    else
                      fields[#fields + 1] = source[j]
                      fields[#fields + 1] = source[j + 1]
                    end
                  end
                  fields[#fields + 1] = 'deliveries'
                  fields[#fields + 1] = tostring(deliveries)
                  redis.call('XADD', KEYS[1], '*', unpack(fields))
                  requeued = requeued + 1
                end
                ack(KEYS[1], ARGV[1], ARGV[2], {id})
              end
            end
            return requeued
            LUA;
    }

    /**
     * Sposta nello stream i differiti scaduti. I membri sono oggetti JSON {campo: valore}.
     *
     * KEYS: delayed, stream. ARGV: istante attuale in ms, limite → numero di messaggi promossi.
     */
    public static function promoteDelayed(): string
    {
        return <<<'LUA'
            local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
            for _, member in ipairs(due) do
              local fields = {}
              for name, value in pairs(cjson.decode(member)) do
                fields[#fields + 1] = name
                fields[#fields + 1] = value
              end
              redis.call('XADD', KEYS[2], '*', unpack(fields))
              redis.call('ZREM', KEYS[1], member)
            end
            return #due
            LUA;
    }

    /**
     * Riaccoda dalla DLQ con attempts = 0, togliendo i campi `failure_*` e `deliveries`.
     *
     * KEYS: dlq, stream. ARGV: `first` + limite, oppure `id` + ID dell'entry → numero di messaggi riaccodati.
     */
    public static function requeueDeadLetters(): string
    {
        return <<<'LUA'
            local entries
            if ARGV[1] == 'id' then
              entries = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2])
            else
              entries = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', ARGV[2])
            end
            for _, entry in ipairs(entries) do
              local fields = {}
              local source = entry[2]
              for j = 1, #source, 2 do
                local name = source[j]
                if name == 'attempts' then
                  fields[#fields + 1] = name
                  fields[#fields + 1] = '0'
                elseif name ~= 'deliveries' and string.sub(name, 1, 8) ~= 'failure_' then
                  fields[#fields + 1] = name
                  fields[#fields + 1] = source[j + 1]
                end
              end
              redis.call('XADD', KEYS[2], '*', unpack(fields))
              redis.call('XDEL', KEYS[1], entry[1])
            end
            return #entries
            LUA;
    }

    /**
     * KEYS: dlq → numero di messaggi rimossi.
     */
    public static function purgeDeadLetters(): string
    {
        return <<<'LUA'
            local count = redis.call('XLEN', KEYS[1])
            redis.call('DEL', KEYS[1])
            return count
            LUA;
    }

    /**
     * Rilascio condizionato del lock (fallback di DELEX ... IFEQ). KEYS: chiave. ARGV: token → 1 se rilasciato.
     */
    public static function releaseLock(): string
    {
        return <<<'LUA'
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            LUA;
    }
}
