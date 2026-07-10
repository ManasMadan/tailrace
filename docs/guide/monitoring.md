# Monitoring

One number matters more than all others: **slot lag**. Everything else is detail.

## Slot lag and the disk-growth risk

A replication slot forces Postgres to retain WAL from its `restart_lsn` forward. That is the durability guarantee — and the hazard. If walcast is stopped, a durable sink is paused, or a consumer is slow, WAL retention grows **without bound** until the disk fills or `max_slot_wal_keep_size` (if you set it) invalidates the slot.

Watch it in the database:

```sql
SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```

Alert on `retained_bytes` above a threshold you can afford. The same figure is in `walcast status` (CLI), `Walcast#status()` (library, as `slot.retainedWalBytes`), and `/api/stats` (daemon).

If you have stopped using walcast **permanently**, drop the slot: `npx walcast teardown`. An orphaned slot is the number one way this tool can hurt you.

## The `/api/stats` endpoint

Daemon mode exposes everything at `GET /api/stats` (bearer token required — see the [HTTP API](/reference/http-api)):

```jsonc
{
  "uptimeMs": 123456,
  "engine": {
    "eventsTotal": 41823, // events fanned out since start
    "flushedLsn": "0/1A2B3C8", // what we've reported durable to Postgres
    "sinks": [
      {
        "id": "sink-webhook",
        "name": "webhook",
        "durability": "durable",
        "status": "running", // or "paused"
        "lastError": null, // last delivery error, if any
        "queueDepth": 12, // buffered events for this sink
        "deliveredCount": 41811,
        "droppedCount": 0, // ephemeral sinks only
        "ackedLsn": "0/1A2B3C8", // this sink's checkpoint
      },
    ],
  },
  "slot": {
    "exists": true,
    "active": true,
    "restartLsn": "0/1A2B000",
    "confirmedFlushLsn": "0/1A2B3C8",
    "retainedWalBytes": 968, // the number to alert on
  },
  "publication": { "exists": true, "allTables": true },
  "walLevel": "logical",
}
```

What to watch:

- **`slot.retainedWalBytes` growing** — something isn't consuming or acking. Check for a paused sink.
- **`sinks[].status == "paused"`** with a `lastError` — a durable sink exhausted `maxAttempts`. It holds the slot until you fix the receiver and `POST /api/sinks/:id/resume`.
- **`queueDepth` pinned at the configured `queueDepth`** — a durable sink is the bottleneck; the engine is backpressuring replication.
- **`droppedCount` climbing** — an ephemeral sink's consumers can't keep up; events are being dropped (which is that sink's contract, but you may still want to know).

## The dashboard

`/ui` (same bearer token, `?token=` accepted for browsers) renders the same stats: engine throughput with a sparkline, slot state, and a per-sink table with pause/resume buttons. It's fully self-contained static assets — no CDN fonts, no external requests — so it works air-gapped next to the database.

## Backpressure behavior

Every buffer in walcast is bounded, and the response to a full buffer is **pause, not drop**:

1. A durable sink's queue fills (default 1,000 events) → the engine stops pulling from the replication iterator.
2. The library's decoded-event queue fills (default 10,000) → the replication socket is paused.
3. Postgres keeps writing WAL server-side; the slot retains it. **Never dropped.**

So a slow durable consumer converts into server-side disk usage, visible as slot lag — the query above is your early warning. Ephemeral sinks are the one exception: they drop rather than backpressure, by contract, and count it in `droppedCount`.

## Health checks

`GET /healthz` — no auth, returns `{ ok: true, uptimeMs }`. Point liveness probes here. It reports the daemon process, not delivery health; for delivery, alert on stats.
