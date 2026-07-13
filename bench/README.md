# @walcast/bench

End-to-end benchmark behind `npm run bench`. Starts (or reuses) throwaway
Postgres 16 and Kafka containers, runs the daemon with webhook + Kafka
(exactly-once) sinks, writes single-row transactions at full speed, and
reports:

- sustained throughput (committed transactions per second)
- commit → webhook latency, p50/p95
- commit → Kafka latency under `read_committed`, p50/p95

```bash
pnpm -r build
npm run bench

# knobs
BENCH_SECONDS=30 BENCH_WRITERS=8 npm run bench
BENCH_DATABASE_URL=... BENCH_KAFKA=host:port npm run bench   # bring your own infra
```

Latency is measured from the event's `commit_time` (assigned by Postgres at
commit) to arrival at the consumer, on the same host clock. Numbers in the
root README come from this script — rerun it on your hardware rather than
trusting ours.
