# Concepts: WAL, slots, LSNs, and the wire

You can use walcast without reading this page. But when something looks wrong at 3am, this is the mental model that makes the logs legible.

## The WAL

Postgres writes every change to the write-ahead log before applying it. Logical replication is a second reader of that log: a `walsender` process decodes WAL records through an output plugin (`pgoutput` for walcast) and streams them to a client. Walcast is that client — a hand-written one, speaking the protocol directly over a `node-postgres` connection opened with `replication: 'database'`.

Two server-side objects make this work:

- **A publication** — a named set of tables whose changes are decoded. `walcast setup` creates one `FOR ALL TABLES` (or for the tables you list).
- **A replication slot** — the server-side cursor. The slot remembers how far its consumer has confirmed (`confirmed_flush_lsn`) and forces the server to retain all WAL after `restart_lsn`. This is both the durability guarantee and the disk-growth hazard: a slot nobody consumes retains WAL forever.

## LSNs

A Log Sequence Number is a byte position in the WAL — an unsigned 64-bit integer. Textually Postgres shows it as two hex halves: `16/B374D848` (high 32 bits, slash, low 32 bits).

Walcast uses `bigint` internally — JS `number` silently loses precision past 2^53, and exact ordering is the whole point — and the familiar `X/Y` text form at every public boundary (events, config, HTTP), because that is what `pg_replication_slots` shows and what you paste into support threads.

```ts
parseLsn('16/B374D848') // (0x16n << 32n) | 0xB374D848n
formatLsn(0n) // '0/0'
```

## pgoutput framing

Inside each replication data frame is one pgoutput message, identified by a single-byte tag:

| Tag | Message  | Contents                                                                |
| --- | -------- | ----------------------------------------------------------------------- |
| `B` | Begin    | commit LSN, commit time, xid — opens a transaction                      |
| `C` | Commit   | commit LSN, **end LSN**, commit time — closes it                        |
| `O` | Origin   | replication origin name (bookkeeping)                                   |
| `R` | Relation | a table's shape: schema, name, replica identity, columns with type OIDs |
| `Y` | Type     | a custom type's name (bookkeeping)                                      |
| `I` | Insert   | relation OID + new tuple                                                |
| `U` | Update   | relation OID + optional old tuple (`K` key / `O` full row) + new tuple  |
| `D` | Delete   | relation OID + old tuple (`K` or `O`)                                   |
| `T` | Truncate | relation OIDs + cascade / restart-identity flags                        |

`Relation` messages arrive before the first change touching a table (and again when its shape changes); the decoder caches them by OID so tuples can be materialized into named-column objects. Change messages for a relation never seen on this connection are a protocol error, and walcast throws.

**TupleData** is a column count followed by, per column, one of:

- `n` — NULL
- `u` — unchanged TOAST column (the value wasn't in the WAL record; walcast surfaces the `UNCHANGED_TOAST` sentinel string — see the [FAQ](/guide/faq#what-is-unchanged-toast))
- `t` — length-prefixed text bytes (walcast runs pgoutput in text format, not binary)

Text values are converted conservatively: `bool`, `int2`, `int4`, `oid`, `float4`, `float8`, `json`, `jsonb` become JS values; **`int8` and `numeric` stay strings** (both can exceed `Number`'s safe range); everything else (timestamps, uuids, arrays, enums, ...) is the Postgres text form.

## The streaming replication protocol

After `START_REPLICATION SLOT "walcast" LOGICAL 0/0 (proto_version '1', publication_names '"walcast"')` the connection enters copy-both mode. Starting at `0/0` means "resume from the slot's `confirmed_flush_lsn`" — the restart-safe default. Three payloads flow inside CopyData frames:

**`w` — XLogData** (server → client): `walStart` (position of this payload), `walEnd` (current end of WAL on the server), send time, then the pgoutput message bytes.

**`k` — Primary keepalive** (server → client): `walEnd`, send time, and a **reply-requested** flag. When the flag is set, the server demands an immediate standby status update — fail to answer long enough and the server terminates the connection.

**`r` — Standby status update** (client → server): three LSNs — **written** (highest received), **flushed** (durably processed; the server may recycle WAL up to here), and **applied** — plus a timestamp and an optional reply-request. Walcast sends `flushed = applied =` its acked frontier, on a timer (default every 10s), immediately when a keepalive requests a reply, and once more on shutdown.

The `flushed` position is the load-bearing field: it must reflect acknowledged work and nothing more optimistic, because the server is free to discard WAL below it. This is why `ack()` exists.

### Idle databases and keepalive positions

Keepalives advance `walEnd` even when your published tables are quiet (other tables, checkpoints, and vacuum still write WAL). Confirming a keepalive's position too eagerly can lose data — frames for an already-committed transaction may still sit in walcast's queue. Walcast advances on keepalives only when it is fully drained: no unacked events, no buffered events, not mid-transaction. Without this, a walcast watching a quiet table on a busy database would retain WAL forever.

## Backpressure, not loss

Frames are decoded eagerly the moment they arrive (so a Commit record advances the slot even while your consumer is between pulls); decoded events buffer in a bounded queue (default high-water mark 10,000). Past the mark, walcast pauses the replication socket. Backpressure flows to Postgres: WAL accumulates on the server — visible as slot lag, [monitor it](/guide/monitoring) — and nothing is ever dropped.

Next: [Delivery guarantees](/guide/delivery-guarantees) — what all of this machinery does and does not promise.
