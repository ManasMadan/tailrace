# @walcast/sink-grpc

Durable gRPC delivery for [walcast](https://github.com/ManasMadan/walcast):
the sink is a gRPC **client** that pushes ordered batches to _your_ server
implementing the published
[`walcast.v1.WalcastSink`](https://github.com/ManasMadan/walcast/blob/master/proto/walcast/v1/sink.proto)
contract (the `.proto` also ships inside this package).

```bash
npm install walcast @walcast/sink-grpc
```

```jsonc
// walcast.config.json
{
  "sinks": [
    {
      "use": "@walcast/sink-grpc",
      "config": {
        "address": "localhost:50051",
        "tls": false, // or { "caFile": "...", "certFile": "...", "keyFile": "..." }
        "deadlineMs": 30000,
      },
    },
  ],
}
```

Your server implements one rpc:

```proto
service WalcastSink {
  rpc Deliver(ChangeEventBatch) returns (DeliverAck);
}
```

Return `ok: true` only after durably processing the batch — anything else
(false ack, error status, deadline) makes the engine retry with backoff.
Batches can be redelivered; deduplicate on `event.id`. A complete runnable
consumer lives in
[`examples/grpc-consumer`](https://github.com/ManasMadan/walcast/tree/master/examples/grpc-consumer).

Docs: https://walcast.mmadan.in/guide/sinks/grpc

## License

MIT
