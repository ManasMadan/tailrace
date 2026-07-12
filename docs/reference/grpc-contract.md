# gRPC contract

The wire contract between `@walcast/sink-grpc` (the client, pushing) and your server (the receiver). The canonical file is `proto/walcast/v1/sink.proto` in the repo; it also ships inside the sink package, whose `PROTO_PATH` export points at it.

```proto
syntax = "proto3";

package walcast.v1;

service WalcastSink {
  // Deliver one ordered batch of change events.
  rpc Deliver(ChangeEventBatch) returns (DeliverAck);
}

message ChangeEventBatch {
  repeated ChangeEvent events = 1;
}

message ChangeEvent {
  // Deterministic id: "<commit_lsn>:<index within transaction>".
  string id = 1;
  // WAL position of this individual change, e.g. "0/1A2B3C4".
  string lsn = 2;
  // Commit LSN of the containing transaction; events are ordered by it.
  string commit_lsn = 3;
  // Transaction commit time, ISO 8601.
  string commit_time = 4;
  string schema = 5;
  string table = 6;
  // insert | update | delete | truncate
  string op = 7;
  // Row images as JSON documents; empty string when absent.
  string before_json = 8;
  string after_json = 9;
}

message DeliverAck {
  bool ok = 1;
  // Optional diagnostic carried into the walcast daemon's logs.
  string message = 2;
}
```

## Walking through it

**`WalcastSink.Deliver`** — a unary RPC per batch. Walcast is the gRPC _client_: it dials your server and pushes; you never poll. Batches arrive in strict commit order, one in flight at a time per sink.

**`ChangeEvent`** — the [standard event](/reference/event-schema) with one representational difference: row images are JSON **strings** (`before_json` / `after_json`), empty string when absent, rather than protobuf `Struct`. JSON keeps every Postgres type representable exactly as the pgoutput text form delivered it — a `Struct` would force lossy coercions (e.g. big `int8` values into doubles). Parse with any JSON library:

```js
const before = wire.before_json ? JSON.parse(wire.before_json) : null
const after = wire.after_json ? JSON.parse(wire.after_json) : null
```

`op` is a string (`insert` | `update` | `delete` | `truncate`), matching the JSON event schema rather than introducing a parallel enum.

**`DeliverAck`** — your durability signal. Return `ok: true` **only after** the batch is durably processed. Everything else — `ok: false`, a non-OK gRPC status, hitting the deadline (default 30s, configurable) — makes the walcast engine retry the batch with exponential backoff and pause the sink after `maxAttempts`. `message` on a failed ack is carried into the daemon's logs (`gRPC consumer rejected the batch: <message>`), so make it useful.

## Semantics for implementers

- **At-least-once.** Batches may be redelivered after crashes or retries. Deduplicate on `ChangeEvent.id` — it is stable across redeliveries and totally ordered (compare by commit LSN as an integer, then index).
- **Ack durability first.** `ok: true` before your own write is durable converts the pipeline to at-most-once.
- **Don't reorder.** Process the batch in the order received if ordering matters downstream.

A minimal runnable implementation lives at `examples/grpc-consumer/server.mjs` in the repo — see the [gRPC sink page](/guide/sinks/grpc) for the walkthrough and TLS/deadline configuration.
