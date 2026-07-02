# gRPC consumer

A runnable server implementing the published
[`walcast.v1.WalcastSink`](../../proto/walcast/v1/sink.proto) contract.
`@walcast/sink-grpc` is a gRPC _client_ — it pushes ordered batches to a
server like this one.

## Run

```bash
pnpm install
node server.mjs            # listens on :50051 (PORT to override)
```

Point the daemon at it:

```jsonc
// walcast.config.json
{
  "sinks": [{ "use": "@walcast/sink-grpc", "config": { "address": "localhost:50051" } }],
}
```

## What it demonstrates

- Loading the `.proto` and serving `Deliver` with `@grpc/grpc-js`
- Acking `ok: true` only after processing — anything else makes the
  walcast engine retry the batch with backoff
- Deduplicating redeliveries on `event.id` (stable across redelivery),
  which is what turns at-least-once delivery into exactly-once processing
- Row images travel as JSON strings (`before_json` / `after_json`) so every
  Postgres type survives exactly as pgoutput delivered it
