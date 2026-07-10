# Production checklist

Short, in rough order of how badly skipping each one hurts.

## 1. `wal_level = logical`

```
# postgresql.conf — requires a restart
wal_level = logical
```

`setup()` verifies this and fails with instructions if it's off. Managed Postgres: RDS uses the `rds.logical_replication` parameter; most other providers have a toggle. Also make sure `max_replication_slots` and `max_wal_senders` have headroom (defaults of 10 are fine for one walcast).

The connecting role needs the `REPLICATION` attribute (or superuser), plus rights to create the publication and — in daemon mode — the `walcast` checkpoint schema.

## 2. Monitor slot lag — the disk is on the line

A replication slot retains WAL until its consumer confirms it. Stopped daemon, paused sink, slow consumer: retention grows until the disk fills. Alert on:

```sql
SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```

See [Monitoring](/guide/monitoring) for the full playbook, including paused-sink detection via `/api/stats`.

## 3. Tear down when you're done

If you stop using walcast permanently:

```bash
npx walcast teardown
```

This drops the slot and publication (after confirmation; `--yes` to skip). Undelivered changes are gone for good — that's why it's manual and confirmed. Never leave an orphaned slot behind; see point 2 for why.

## 4. REPLICA IDENTITY for the tables you diff

Default replica identity gives you `before: null` on updates and key-only `before` on deletes. If consumers need previous row images:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

Per table, and it increases WAL volume for writes to that table. Decide deliberately; don't blanket-apply it to a write-heavy schema without checking the cost.

## 5. Pin the auth token

Without configuration the daemon generates a fresh admin token per start and prints it — fine for a laptop, wrong for production (restarts invalidate dashboards, tokens end up in logs). Pin one:

```bash
WALCAST_AUTH_TOKEN=$(openssl rand -base64 32)
```

or `server.authToken` in the config file. The daemon binds `127.0.0.1` by default; if you change `server.host`, put TLS in front (a reverse proxy) — the daemon itself speaks plain HTTP, and `?token=` in URLs is only acceptable over TLS.

## 6. One instance per slot

Postgres enforces a single consumer per replication slot: a second `walcast serve` against the same slot fails to start replication ("slot is active"). So:

- Run **one** daemon per slot. For independent pipelines, use distinct `slot` + `publication` names.
- Don't run library-mode consumers against the daemon's slot.
- There is no HA/failover story yet — a supervisor that restarts the single instance (the engine reconnects with backoff and redelivery is safe) is the current answer.

## 7. The boring rest

- **Batching/retry tuning** (`engine.batchSize`, `lingerMs`, `maxAttempts`, `queueDepth`) — defaults are sane; see the [config reference](/reference/config) before touching them.
- **Consumer idempotency** — at-least-once means duplicates happen. Every receiver dedupes on `event.id`. If a duplicate would page you, do it now, not after the first crash. ([Why](/guide/delivery-guarantees).)
- **Restore drills** — daemon checkpoints live in the `walcast` schema of the source database, so a restore restores consistent checkpoints; but a restored database has _no slot_ (slots aren't in backups). After a restore, `walcast setup` recreates it and streaming resumes from the new slot's position — changes between backup and restore were never in the WAL you have. Know this before you need it.
