# gRPC sink

`@walcast/sink-grpc` — durable push to a gRPC server **you** implement. Walcast is the client; your server implements the published `walcast.v1.WalcastSink` contract and acks batches.

```bash
npm install @walcast/sink-grpc
```

```json
{
  "sinks": [
    {
      "use": "@walcast/sink-grpc",
      "config": { "address": "localhost:50051" }
    }
  ]
}
```

## Config

| Key          | Type                                        | Default             | Description                                                                              |
| ------------ | ------------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------- |
| `address`    | `string`                                    | required            | `host:port` of your server implementing `walcast.v1.WalcastSink`.                      |
| `tls`        | `false \| { caFile?, certFile?, keyFile? }` | `false` (plaintext) | TLS off, or paths to CA / client cert / client key files (mTLS when cert+key are given). |
| `deadlineMs` | `number`                                    | `30000`             | Per-call deadline. A missed deadline counts as a failed attempt.                         |

## Semantics

Durable, with exactly the webhook sink's ordering and redelivery semantics on a different wire:

- One `Deliver` RPC per batch, batches in strict commit order.
- Return `ok: true` **only after** you have durably processed the batch. Anything else — `ok: false`, a non-OK status, a missed deadline — makes the engine retry with backoff and pause the sink after `maxAttempts`.
- At-least-once: batches may be redelivered. Deduplicate on `ChangeEvent.id`, which is stable across redeliveries.

Row images travel as JSON strings (`before_json` / `after_json`, empty string when absent) rather than protobuf `Struct` — JSON keeps every Postgres type representable exactly as the pgoutput text form delivered it. Full message definitions: [gRPC contract reference](/reference/grpc-contract).

## A minimal consumer

The repo ships a runnable one at `examples/grpc-consumer/server.mjs`:

```js
import grpc from '@grpc/grpc-js'
import protoLoader from '@grpc/proto-loader'

const definition = protoLoader.loadSync(PROTO_PATH, { keepCase: true, defaults: true })
const { walcast } = grpc.loadPackageDefinition(definition)

// Idempotency: remember processed event ids (use your database in real life).
const processed = new Set()

const server = new grpc.Server()
server.addService(walcast.v1.WalcastSink.service, {
  Deliver(call, callback) {
    try {
      for (const wire of call.request.events) {
        if (processed.has(wire.id)) continue // redelivery — already handled
        const after = wire.after_json ? JSON.parse(wire.after_json) : null
        console.log(wire.op, `${wire.schema}.${wire.table}`, wire.id, after)
        processed.add(wire.id)
      }
      callback(null, { ok: true, message: '' })
    } catch (err) {
      // Anything not-ok makes walcast retry the batch with backoff.
      callback(null, { ok: false, message: String(err) })
    }
  },
})

server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {})
```

The `.proto` file ships inside the sink package; `@walcast/sink-grpc` exports its path as `PROTO_PATH` if you want to load the same definition your daemon uses.
