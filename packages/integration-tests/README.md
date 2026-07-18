# @walcast/integration-tests

Cross-package tests that don't belong to any single package. Never
published.

- **Daemon behavior** — the zero-sink onboarding error, config-driven
  plugin loading from a user-style project
- **Money test #1** — continuous writes, `kill -9` the daemon mid-stream,
  restart: every committed row reaches the webhook at least once (duplicates
  allowed, gaps are a failure)
- **Money test #2** — continuous writes, `kill -9` during Kafka
  transactions, restart: a `read_committed` consumer sees exactly one copy
  of every committed row (duplicates _and_ gaps are failures)

```bash
pnpm -r build                      # tests run the built CLI like a user would
pnpm --filter @walcast/integration-tests test
```

Requires docker: the suite starts throwaway Postgres 16 and Kafka (KRaft)
containers and removes them afterwards. Without docker it skips itself.
`WALCAST_TEST_DSN` / `WALCAST_TEST_KAFKA` point it at existing services
(that's what CI service containers would use).
