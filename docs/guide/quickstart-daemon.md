# Quickstart: daemon mode

Daemon mode is for when something else should receive events on your behalf — a webhook endpoint, a Kafka topic, a gRPC service, a browser. The daemon runs the sink engine (ordering, batching, retries, checkpoints) and exposes a control plane: an admin API and a dashboard.

The core ships zero sinks. Run `npx walcast serve` with no sinks configured and you get this — the error message is the onboarding:

```
walcast daemon needs at least one sink plugin — the core transports nothing by itself.

Install one and add it to walcast.config.json:

  npm install @walcast/sink-webhook     HTTP POST delivery (HMAC-signed, durable)
  npm install @walcast/sink-sse         live Server-Sent Events endpoint (ephemeral)
  npm install @walcast/sink-kafka       Kafka, exactly-once into the topic (durable)
  npm install @walcast/sink-grpc        push batches to your gRPC server (durable)

Example config:

  {
    "sinks": [
      {
        "use": "@walcast/sink-webhook",
        "config": { "url": "https://example.com/hooks/walcast", "secret": "..." }
      }
    ]
  }

Writing your own transport is a ~50-line plugin: https://github.com/ManasMadan/walcast/tree/master/templates/plugin

(If you just want events in your own code, you don't need the daemon at all:
  import { Walcast } from 'walcast'  — your code is the sink.)
```

## 1. Install a sink

```bash
npm install walcast @walcast/sink-webhook
```

Sinks are installed in _your_ project; the daemon resolves them from the working directory (a local file like `./my-sink.js` works too).

## 2. Write `walcast.config.json`

```json
{
  "db": "postgres://user:pass@localhost:5432/mydb",
  "sinks": [
    {
      "use": "@walcast/sink-webhook",
      "config": {
        "url": "https://example.com/hooks/walcast",
        "secret": "your-hmac-secret"
      }
    }
  ]
}
```

Environment variables override the file: `DATABASE_URL` / `WALCAST_DB`, `WALCAST_PORT`, `WALCAST_AUTH_TOKEN`, and more — see the [config reference](/reference/config).

## 3. Start it

```bash
npx walcast serve
```

The daemon runs `setup()` (idempotent), loads each sink, starts the engine, and listens on `127.0.0.1:7717` by default. On startup it logs something like:

```
walcast daemon started  port=7717 sinks=["sink-webhook (durable)"] publication=walcast slot=walcast
generated admin token (set server.authToken or WALCAST_AUTH_TOKEN to pin one)  token=xxxxxxxx
dashboard: http://127.0.0.1:7717/ui/?token=xxxxxxxx
```

## 4. Open the dashboard

The admin API and dashboard are protected by a bearer token. If you didn't set one (`server.authToken` in the config, or `WALCAST_AUTH_TOKEN`), a fresh token is generated per start and printed in the log. Open the printed URL:

```
http://127.0.0.1:7717/ui/?token=<token>
```

The dashboard shows engine throughput, slot lag, and per-sink status with pause/resume. The same data is available from the API:

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:7717/api/stats
```

`GET /healthz` needs no auth. Full surface: [HTTP API reference](/reference/http-api).

## What the engine guarantees

- Events fan out to every sink **in commit order**.
- **Durable sinks** get bounded queues; when a queue fills, the engine stops reading from replication — WAL accumulates on the server, never dropped. Failed deliveries retry with exponential backoff and jitter; after `maxAttempts` (default 10) the sink is **paused** with its last error, holding the slot until you resume it from the API/UI.
- **Ephemeral sinks** (like SSE) are best-effort and can never hold WAL back.
- The replication slot advances to the minimum acknowledged LSN across durable sinks only. Per-sink checkpoints are stored in a `walcast` schema inside your source database — nothing extra to run.

Next: pick a sink — [webhook](/guide/sinks/webhook), [SSE](/guide/sinks/sse), [Kafka](/guide/sinks/kafka), [gRPC](/guide/sinks/grpc) — or [write your own](/guide/writing-a-sink).
