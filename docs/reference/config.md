# Configuration

Daemon configuration lives in `walcast.config.json` (override the path with `walcast serve --config <path>`). A missing file is fine as long as the environment provides a database. **Environment variables win over the file.**

## Full schema

```jsonc
{
  // Postgres connection string. Required (here or via env).
  "db": "postgres://user:pass@localhost:5432/mydb",

  // Publication and slot names. Defaults: "walcast" / "walcast".
  "publication": "walcast",
  "slot": "walcast",

  "server": {
    // Admin API + dashboard port. Default 7717. 0 = random free port.
    "port": 7717,
    // Bind address. Default 127.0.0.1 — loopback only.
    "host": "127.0.0.1",
    // Bearer token for /api and /ui. Default null = generated per start
    // and printed in the log. Pin one in production.
    "authToken": null,
  },

  "engine": {
    // Max events per deliver() batch. Default 100.
    "batchSize": 100,
    // How long to wait for a fuller batch after the first event, ms. Default 25.
    "lingerMs": 25,
    // Delivery attempts before a durable sink is paused. Default 10.
    "maxAttempts": 10,
    // Per-sink buffered events before backpressure (durable) / drops (ephemeral). Default 1000.
    "queueDepth": 1000,
  },

  // At least one sink required for `serve`.
  "sinks": [
    {
      // Package to import — or a local path like "./my-sink.js".
      // Resolved from the working directory (your project's node_modules).
      "use": "@walcast/sink-webhook",
      // Instance id; defaults to the package name (with @walcast/ stripped,
      // e.g. "sink-webhook"). Must be unique — required when using the same
      // package twice.
      "name": "orders-hook",
      // Passed to the sink's factory and exposed as ctx.config.
      "config": { "url": "https://example.com/hooks/walcast" },
    },
  ],
}
```

## Environment overrides

| Variable               | Overrides          | Notes                              |
| ---------------------- | ------------------ | ---------------------------------- |
| `WALCAST_DB`          | `db`               | wins over `DATABASE_URL`           |
| `DATABASE_URL`         | `db`               | conventional fallback              |
| `WALCAST_PORT`        | `server.port`      |                                    |
| `WALCAST_AUTH_TOKEN`  | `server.authToken` |                                    |
| `WALCAST_PUBLICATION` | `publication`      |                                    |
| `WALCAST_SLOT`        | `slot`             |                                    |
| `WALCAST_LOG_LEVEL`   | log level          | `debug` or `info` (default `info`) |

No database from any source is a startup error; no sinks is the [zero-sink onboarding error](/guide/quickstart-daemon).

## Engine settings, when to touch them

- **`batchSize` / `lingerMs`** — throughput vs latency. Bigger batches amortize per-request overhead (webhook HTTP round-trips, Kafka transactions); `lingerMs` bounds how long the first event of a batch waits for company. Defaults suit most pipelines.
- **`maxAttempts`** — with exponential backoff (base 200ms, cap 30s, full jitter), 10 attempts spread over roughly a minute and a half. After that the sink pauses with its last error and **holds the slot** until resumed — deliberate: silent skipping is data loss.
- **`queueDepth`** — per-sink buffer. For durable sinks it bounds memory before the engine backpressures replication (WAL then accumulates server-side); for ephemeral sinks it's the drop threshold.

## Library-mode options

Library mode doesn't read the config file. `new Walcast(options)` takes:

| Option             | Default      | Description                                             |
| ------------------ | ------------ | ------------------------------------------------------- |
| `connection`       | required     | connection string or `pg.ClientConfig`                  |
| `publication`      | `'walcast'` | publication name                                        |
| `slot`             | `'walcast'` | slot name                                               |
| `tables`           | all tables   | restricts the publication when `setup()` creates it     |
| `statusIntervalMs` | `10000`      | standby status update interval                          |
| `highWaterMark`    | `10000`      | buffered events before the replication socket is paused |
