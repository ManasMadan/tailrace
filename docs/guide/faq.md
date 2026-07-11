# FAQ

## What is `UNCHANGED_TOAST`? {#what-is-unchanged-toast}

Postgres stores large column values out-of-line ("TOAST"). An `UPDATE` that doesn't touch a TOASTed column doesn't include its value in the WAL record at all — pgoutput marks it `u` (unchanged) instead. Walcast surfaces that as an exported string constant:

```ts
import { UNCHANGED_TOAST } from 'walcast' // "__walcast:unchanged_toast__"

if (event.after?.big_json_column === UNCHANGED_TOAST) {
  // value unchanged by this update and not present in the WAL —
  // fetch from the table if you actually need it
}
```

Why a weird string and not a `Symbol`? A Symbol would be more elegant in-process but silently disappears through `JSON.stringify` — and every daemon sink serializes events. A visible sentinel is debuggable; a missing key is a data-loss bug report.

## Why is `before` null on my updates?

Default replica identity. Postgres only writes the old row image to the WAL when the table's `REPLICA IDENTITY` says to:

- `DEFAULT` — old image only for deletes, and only key columns
- `FULL` — full old row for updates and deletes

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

This is per-table, costs extra WAL on writes, and is a Postgres setting — walcast can only deliver what's in the WAL.

## Why do `bigint` and `numeric` columns arrive as strings?

Both can exceed `Number.MAX_SAFE_INTEGER` / lose precision as a float. A CDC pipeline that silently corrupts big ids is worse than one that hands you strings — so walcast converts only what is loss-free (`bool`, `int2`, `int4`, `oid`, `float4`, `float8`, `json`, `jsonb`) and leaves the rest in Postgres text form. Parse `int8` with `BigInt(value)` and `numeric` with your decimal library of choice.

## Why do arrays / timestamps / uuids arrive as strings?

Same policy. pgoutput (in text mode) ships every value as Postgres text; anything without a guaranteed loss-free JS mapping stays that way. Arrays are Postgres array literals (`{a,b}`), timestamps are like `2026-07-19 12:00:00+00`. Predictable beats clever.

## Why is the schema field sometimes `pg_catalog`-looking or empty?

pgoutput sends an empty namespace for `pg_catalog`; walcast normalizes it to `pg_catalog` in the relation cache. User tables always carry their real schema (`public`, etc.).

## I see events for a `walcast` schema I never created

Daemon mode stores per-sink checkpoints in a `walcast` schema inside your source database. With a `FOR ALL TABLES` publication those checkpoint writes would themselves generate change events whose delivery writes checkpoints — an infinite feedback loop — so the engine never fans out events from the `walcast` schema. In library mode with `FOR ALL TABLES` you may see them if you also run a daemon against the same database; filter on `event.schema` if so.

## Does walcast reconnect after a network blip?

Depends on the mode, deliberately:

- **Library mode: no.** `changes()` throws. A library must not silently mask errors or secretly retry. Restart the iteration yourself — at-least-once semantics make that always safe.
- **Daemon mode: yes.** The engine wraps the library in reconnect-with-backoff, because a daemon's job is to stay up.

## Can I run two walcast instances for HA?

Not against the same slot — Postgres enforces one consumer per slot, the second instance fails to attach. No failover story yet; run one instance per slot under a supervisor. Independent pipelines can each have their own slot + publication.

## Where did my events go after `teardown`?

Dropping the slot releases the retained WAL — undelivered changes are gone for good. That's why `teardown` asks for confirmation. Durability lives in the slot.

## Does walcast capture DDL / schema changes?

No. Logical replication decodes DML (insert/update/delete/truncate). Column additions show up implicitly — the next change to that table carries the new shape (pgoutput re-sends the Relation message).

## What Postgres versions work?

Anything speaking pgoutput `proto_version 1` — Postgres 10+. The test suite runs against Postgres 16.
