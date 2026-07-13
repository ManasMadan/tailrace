# Demo

The `docker compose up` showcase: Postgres 16 (`wal_level=logical`),
single-node Kafka (KRaft), a stand-in webhook receiver, and the walcast
daemon built from this workspace with SSE + webhook + Kafka sinks.

```bash
docker compose up --build     # from the repo root
npm run demo                  # seed an orders table, generate scripted writes
open http://127.0.0.1:7717/ui/?token=demo
```

What to look at:

- **Live inspector** — inserts/updates/deletes streaming as they commit
  (the seed table uses `REPLICA IDENTITY FULL`, so updates carry before
  images)
- **Overview** — the flow rail advancing, events/sec
- `docker compose logs receiver` — signed webhook batches arriving
- Kafka topics `demo.public.orders` on `127.0.0.1:19094` — exactly-once
  under `read_committed`

Tear it down with `docker compose down -v`.
