# Why walcast

> **The core does the least, best, for the most. Everything that transports events is a plugin.** Walcast ships zero sinks — the same way `@babel/core` ships zero transforms and PostCSS ships zero plugins.

That sentence is the whole design. Everything below is consequences.

## The microkernel split

Change data capture has two genuinely hard problems and one endlessly varied one.

The hard problems are **correctness** (decode pgoutput exactly, order events by commit, never advance the replication slot past unacknowledged work, never lose or duplicate silently) and **operations** (backpressure, retries, checkpoints, observability). These live in the core, once, tested against a real walsender.

The endlessly varied problem is **where the events go**. Webhooks today, Kafka tomorrow, Meilisearch next quarter, some internal gRPC service the quarter after. No core can ship every transport, and a core that ships five blesses five and abandons the rest. So walcast's core transports nothing:

- **Data plane = plugins, always.** Every byte that leaves the daemon travels through a `Sink` — a package with a default-export factory implementing four members: `name`, `durability`, `init`, `deliver`, `close`. The official sinks (`@walcast/sink-webhook`, `-sse`, `-kafka`, `-grpc`) are built on exactly the same contract a community plugin uses, verified by the same [conformance harness](/guide/writing-a-sink#verify-it-with-the-conformance-harness).
- **Control plane = core.** The admin API, the bearer-token auth, the dashboard, pause/resume, per-sink checkpoints — that's operating the pipeline, not transporting data, and it belongs to the daemon itself.
- **Library mode is the zero-plugin experience.** If events should land in _your own code_, you don't need the daemon or any plugin at all. `Walcast.changes()` is an async iterator; your loop body is the sink.

## What the core actually owns

The engine owns ordering, batching, retries, backpressure, and per-sink LSN checkpointing. Plugins own transport — nothing else. A sink never decides what "delivered" means for the slot, never reorders, never manages its own retry loop. It serializes and ships, and throws when the far end is unhappy. That narrowness is what makes a sink ~50 lines and makes its failure modes predictable: the engine's behavior on a throwing durable sink is documented once, centrally, and identical for every transport.

## What walcast refuses to do

- **Refuses to claim exactly-once delivery** for webhooks or SSE, because it is [impossible](/guide/delivery-guarantees). It gives you the tools for exactly-once _processing_ instead.
- **Refuses to convert column values lossily.** `int8` and `numeric` arrive as strings; a CDC pipeline that silently corrupts big ids is worse than one that hands you text.
- **Refuses to hide replication failures in library mode.** `changes()` throws; a library must not secretly retry. The daemon engine reconnects with backoff, because a daemon's job is to stay up — and at-least-once semantics make reconnect-and-redeliver always safe.
- **Refuses extra infrastructure.** Daemon checkpoints live in a `walcast` schema inside the source database. A database restore restores consistent checkpoints; there is nothing else to run.

## Where the name comes from

The name is what it does: it broadcasts the WAL. Postgres writes every change to its write-ahead log; walcast decodes that log and casts each change to wherever you point it.

Next: [Quickstart in library mode](/guide/quickstart-library), or [the daemon](/guide/quickstart-daemon) if something else should receive events for you.
