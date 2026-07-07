# daemon-config

A complete, annotated `walcast.config.json` running all four first-party sinks side by side: SSE for watching, webhook + Kafka + gRPC for delivering. JSON has no comments, so every field is documented below.

Run from this directory (the daemon reads `walcast.config.json` from the working directory; `--config <path>` overrides):

```sh
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
npm start
```

## Every field

| Field                | Default       | Meaning                                                                                                                         |
| -------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `db`                 | — (required)  | Postgres connection string. Needs `wal_level=logical`.                                                                          |
| `publication`        | `"walcast"`  | Publication name; created by `walcast setup` / on serve if missing.                                                            |
| `slot`               | `"walcast"`  | Replication slot name. One slot = one consumer.                                                                                 |
| `server.port`        | `7717`        | Admin API + dashboard port. `0` = random free port.                                                                             |
| `server.host`        | `"127.0.0.1"` | Bind address. Loopback only by default.                                                                                         |
| `server.authToken`   | `null`        | Bearer token for `/api` and `/ui`. `null` = a fresh token is generated per start and printed in the log; pin one in production. |
| `engine.batchSize`   | `100`         | Max events per `deliver()` batch.                                                                                               |
| `engine.lingerMs`    | `25`          | How long the first event of a batch waits for company.                                                                          |
| `engine.maxAttempts` | `10`          | Delivery attempts (exponential backoff, full jitter) before a durable sink is paused — never skipped.                           |
| `engine.queueDepth`  | `1000`        | Per-sink buffered events before backpressure (durable) / drops (ephemeral).                                                     |
| `sinks[].use`        | —             | Package to import, or a local path like `"./my-sink.js"`. Resolved from the working directory.                                  |
| `sinks[].name`       | package name  | Instance id; must be unique. Required to use the same package twice.                                                            |
| `sinks[].config`     | `{}`          | Passed to the sink's factory; each sink documents its own shape.                                                                |

## Environment overrides (env wins over the file)

| Variable               | Overrides                       |
| ---------------------- | ------------------------------- |
| `WALCAST_DB`          | `db` (wins over `DATABASE_URL`) |
| `DATABASE_URL`         | `db` (conventional fallback)    |
| `WALCAST_PORT`        | `server.port`                   |
| `WALCAST_AUTH_TOKEN`  | `server.authToken`              |
| `WALCAST_PUBLICATION` | `publication`                   |
| `WALCAST_SLOT`        | `slot`                          |
| `WALCAST_LOG_LEVEL`   | log level (`debug` or `info`)   |

## The four sinks in this config

| Sink                     | Durability | What it does                                                                                                                            |
| ------------------------ | ---------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `@walcast/sink-sse`     | ephemeral  | Live tail at `GET /plugins/live-tail/events` (Server-Sent Events). Never holds the slot back; a disconnected client just misses events. |
| `@walcast/sink-webhook` | durable    | HMAC-signed JSON batches POSTed to your endpoint — see the [webhook-receiver](../webhook-receiver) example.                             |
| `@walcast/sink-kafka`   | durable    | Exactly-once into per-table topics via transactions — see the [kafka-consumer](../kafka-consumer) example.                              |
| `@walcast/sink-grpc`    | durable    | Pushes batches to your `walcast.v1.WalcastSink` server — see the [grpc-consumer](../grpc-consumer) example.                           |

Durable sinks each keep their own checkpoint; the slot only advances past what _every_ durable sink has acknowledged, so one slow consumer never loses another's data.
