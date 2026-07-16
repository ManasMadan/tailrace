# Roadmap

What's deliberately not in v1, why, and how each would land. Items here are
design-aware: the current architecture was shaped so none of them require a
rewrite. If you want to work on one, open an issue first so we can agree on
the approach.

## Active/standby failover (multi-node HA)

A Postgres replication slot allows exactly one consumer at a time, so
running two daemons today is not "HA", it's a fight over
`START_REPLICATION` (the loser gets `replication slot ... is active`).

Planned design: leader election on a Postgres advisory lock, no external
coordinator.

- Every instance runs `SELECT pg_advisory_lock(...)` on a key derived from
  the slot name and blocks until it holds the lock.
- The lock holder streams; standbys sit in the blocking call costing
  nothing.
- When the leader dies, its session ends, the lock releases, and a standby
  takes over. The slot's `confirmed_flush_lsn` plus per-sink checkpoints in
  `walcast.sinks` give the successor an exact resume point; at-least-once
  semantics absorb the handover.

The engine already keeps all delivery state in Postgres (not process
memory) for this reason. What's missing is the election loop, fencing for
the admin API (only the leader should accept pause/resume), and tests for
the handover window.

## pgoutput proto_version 2+ (streaming in-progress transactions)

v1 delivers a transaction only after COMMIT. Postgres 14+ can stream very
large transactions before they commit (`proto_version '2'`,
`streaming 'on'`), which caps memory for multi-gigabyte transactions.

The cost is real new protocol surface: Stream Start/Stop/Commit/Abort
messages, interleaved transactions keyed by xid, and buffering with
discard-on-abort — deliver-before-commit would break the "events you see
are committed" guarantee, so streamed changes must spill somewhere until
the commit arrives. The decoder's message framing was written so the new
tags slot in next to the existing ones; the buffering policy (memory with
disk spill above a threshold) is the actual design work.

## More official sinks

The point of the plugin contract is that most transports should _not_ be
official. Candidates that may graduate based on demand: NATS JetStream,
Redis Streams, SQS. Everything else belongs in community packages —
see the plugin template and the community sinks page. Good first plugins:
a Discord notifier, an NDJSON file sink, a Meilisearch indexer.

## Drizzle support in typegen

`@walcast/typegen-prisma` parses Prisma schemas only. Drizzle schemas are
TypeScript, so the honest implementation imports the schema module and
reads table metadata rather than regex-parsing source. Separate package
(`@walcast/typegen-drizzle`) when it happens.

## Not planned

- **Exactly-once delivery for webhooks/SSE.** Impossible, not merely hard:
  the receiver can crash between processing and acknowledging, and no
  protocol removes that window. walcast ships deterministic event ids so
  receivers can deduplicate; that is the correct fix.
- **Multiple Postgres versions below 14.** pgoutput on 14+ keeps the
  decoder honest; older versions lack features the roadmap depends on.
- **A plugin marketplace/registry service.** npm with the `walcast-sink`
  keyword is the registry.
