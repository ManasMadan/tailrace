# Decisions

Non-obvious choices and the reasoning behind them, newest last. If you are
wondering "why is it like this?", the answer should be here.

## LSNs are `bigint` internally, `X/Y` strings at every boundary

The wire protocol uses unsigned 64-bit integers; JS `number` silently loses
precision past 2^53. `bigint` gives exact comparison/ordering for free.
Public APIs (events, config, HTTP) use the familiar Postgres text form
(`16/B374D848`) because that is what `pg_replication_slots` shows and what
people paste into support threads.

## Column values: convert only what is loss-free, keep the rest as text

pgoutput (without the `binary` option) ships column values as Postgres text.
We convert `bool`, `int2`, `int4`, `oid`, `float4`, `float8`, `json`,
`jsonb`. We deliberately do **not** convert `int8` or `numeric` — both can
exceed `Number`'s safe range, and a CDC pipeline that silently corrupts big
ids is worse than one that hands you strings. Everything else (timestamps,
uuids, arrays, enums, ...) is the Postgres text form.

## Unchanged TOAST columns are a string sentinel, not a symbol

An UPDATE that doesn't touch a TOASTed column does not include its value in
the WAL record. We surface that as the exported constant
`UNCHANGED_TOAST = "__walcast:unchanged_toast__"`. A `Symbol` would be more
elegant in-process but silently disappears through `JSON.stringify`, and
every daemon sink serializes events. A visible weird string is debuggable;
a missing key is a data-loss bug report.

## Frames are decoded eagerly (pump), not lazily on iterator pull

The first implementation decoded a frame only when the consumer pulled the
next event. That meant a Commit record sitting in the socket did not advance
the slot until the _next_ pull — a consumer that processes one event and
waits would hold `confirmed_flush` back a full transaction. The pump decodes
frames the moment they arrive, buffers decoded events in a bounded queue,
and pauses the socket past the high-water mark: backpressure flows to
Postgres, WAL accumulates on the server (documented), and nothing is dropped.

## `ack()` is cumulative, like a Kafka offset commit

Acking event N acknowledges everything ≤ N. Per-event acking with gaps
cannot be expressed to Postgres anyway — a slot has exactly one
`confirmed_flush_lsn` — so pretending to support out-of-order acks would
just defer the surprise. Acking a transaction's last event advances the slot
past the whole transaction (the engine upgrades that event's ack target to
the commit's end LSN when it sees the Commit record).

## Idle keepalive positions are confirmed only when fully drained

Confirming a keepalive's `walEnd` too eagerly can lose data (frames for an
already-committed transaction may still be in our queue). We advance on
keepalives only when: no unacked events, no buffered events, and not
mid-transaction — evaluated when the keepalive comes up in the ordered
frame queue, per the previous section. Without this advancement, a walcast
watching a quiet table on a busy database would retain WAL forever.

## `moduleResolution: Bundler` + `@/*` import aliases

Packages are bundled by tsup (esbuild resolves tsconfig paths natively), so
we don't need Node-style relative specifiers with `.js` extensions in
source. `@/foo` reads better than `../../foo` and survives file moves.
Published artifacts are self-contained ESM with bundled `.d.ts`.

## pnpm workspaces + changesets + husky/commitlint

The standard modern OSS monorepo toolchain (Vite, Astro, tRPC use the same
shape): pre-commit lint-staged, conventional commits enforced at commit-msg,
changesets for per-package versioning and changelogs.

## The event schema and Sink contract live in @walcast/plugin-kit

Plugin authors depend only on the kit, so the kit must own the contract —
including `ChangeEvent`. The core depends on the kit (types only at runtime)
and re-exports the type, rather than maintaining a structural twin that
would drift.

## Checkpoints live in a `walcast` schema inside the source database

No extra infrastructure, and a database restore restores consistent
checkpoints. Consequence discovered by the first end-to-end run: with a
FOR ALL TABLES publication, checkpoint UPDATEs generate change events whose
delivery writes checkpoints — an infinite feedback loop. The engine
therefore never fans out events from the `walcast` schema; they flow
through durable sink queues as skip markers (so cumulative acks can't jump
over queued user events) and are acked like delivered work.

## The engine reconnects; the library does not

`Walcast.changes()` surfaces replication failures to the caller — a
library must not silently mask errors or secretly retry. The daemon engine
wraps the library in a reconnect-with-backoff loop, because a daemon's job
is to stay up (and a kill -9'd predecessor can hold the slot for a moment).
At-least-once semantics make reconnect-and-redeliver always safe.

## Dashboard: no chart library, no font CDN, no router

The daemon may run air-gapped next to a database; the UI is fully
self-contained static assets (system font stacks, hand-rolled SVG
sparkline, tab state in React). React + Tailwind are the only substantial
dependencies, and they're dev-time — the shipped artifact is static files.

## Kafka EOS: checkpoint record in the same transaction as the data

`@walcast/sink-kafka` commits each batch and a `{sinkId → last event id}`
record to a compacted checkpoint topic inside one Kafka transaction. Crash
before commit: aborted, invisible, redelivered — no dupes. Crash after
commit but before the engine ack: redelivered, but the sink reads the
checkpoint back at startup and skips everything at or below it — no dupes.
The same trick resolves an _ambiguous_ commit without a crash: when
`commit()` errors (response lost mid-flight), the sink re-reads the
checkpoint topic before surfacing the failure — if the transaction actually
landed, the checkpoint says so and the engine's retry is filtered instead
of double-produced. The fixed transactional.id fences zombie producers.
kafkajs (pure JS) over librdkafka bindings: no native build step for
installers, and transactions are all we need from the client.

## Single-broker KRaft test topology needs two listeners

The broker's own transaction coordinator dials the _advertised_ listener.
With only a host-mapped advertised address (127.0.0.1:19092), that dial
fails inside the container and every transactional produce dies with
NOT_ENOUGH_REPLICAS. Test containers therefore advertise an INTERNAL
listener for the broker itself and an EXTERNAL one for the host.

## Test Postgres is a throwaway docker container, not a mock

The replication protocol and pgoutput have byte-level unit tests from
hand-built fixtures, but ack semantics, slot advancement, and redelivery are
only meaningful against a real walsender. `test/global-setup.ts` starts
`postgres:16-alpine` with `wal_level=logical` (skips the suite when docker
is unavailable, honors `WALCAST_TEST_DSN` in CI).
