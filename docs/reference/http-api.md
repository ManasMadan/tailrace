# HTTP API

The daemon's control plane. Default bind: `127.0.0.1:7717`.

## Authentication

Everything except `/healthz` requires the admin token, one of:

- `Authorization: Bearer <token>` header
- `?token=<token>` query parameter — for browser contexts that can't set headers (`EventSource`, dashboard assets). Only sensible over TLS.

The token is `server.authToken` from the config / `WALCAST_AUTH_TOKEN` from the environment; if neither is set, a random token is generated per start and printed in the startup log. Failed auth returns `401 {"error":"unauthorized"}`.

## Endpoints

### `GET /healthz` — no auth

```json
{ "ok": true, "uptimeMs": 123456 }
```

Liveness only; says nothing about delivery health.

### `GET /api/stats`

The full picture: uptime, engine stats, slot state, publication, `wal_level`.

```jsonc
{
  "uptimeMs": 123456,
  "engine": {
    "eventsTotal": 41823,
    "flushedLsn": "0/1A2B3C8",
    "sinks": [
      {
        "id": "sink-webhook",
        "name": "webhook",
        "durability": "durable", // "durable" | "ephemeral"
        "status": "running", // "running" | "paused"
        "lastError": null,
        "queueDepth": 12,
        "deliveredCount": 41811,
        "droppedCount": 0,
        "ackedLsn": "0/1A2B3C8",
      },
    ],
  },
  "slot": {
    "exists": true,
    "active": true,
    "restartLsn": "0/1A2B000",
    "confirmedFlushLsn": "0/1A2B3C8",
    "retainedWalBytes": 968,
  },
  "publication": { "exists": true, "allTables": true },
  "walLevel": "logical",
}
```

`slot`/`publication`/`walLevel` are `null` if the status query against Postgres fails (the endpoint still answers with engine stats).

### `GET /api/sinks`

Just the per-sink array: `{ "sinks": [ ...same objects as engine.sinks above ] }`.

### `POST /api/sinks/:id/pause`

Pause a sink. Its queue keeps filling; when full, the engine backpressures replication — a paused **durable** sink holds the slot and retains WAL. Returns `{ "ok": true }`, or `404` with the error for an unknown id.

### `POST /api/sinks/:id/resume`

Resume a paused sink. A sink paused by delivery failures retries its pending batch immediately. Returns `{ "ok": true }` or `404`.

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:7717/api/sinks/sink-webhook/resume
```

### `/plugins/<sinkId>/<path>` — plugin routes

Sinks can mount HTTP routes at init via `ctx.http.registerRoute(path, handler)`; they appear namespaced under `/plugins/<sinkId>` and sit **behind the same auth**. Example: the SSE sink's stream at `GET /plugins/sink-sse/events`. Methods and semantics are the plugin's business; consult the sink's page.

### `/ui` — dashboard

Static single-page dashboard (auth via `?token=`). `GET /` redirects to `/ui/`. Assets are fully self-contained — no external requests — so it works air-gapped.

## Errors

JSON everywhere: `401 {"error":"unauthorized"}`, `404 {"error":"not found"}` (or the specific message for sink actions), `500 {"error":"internal error"}`.
