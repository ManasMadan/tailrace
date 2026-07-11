# CLI

```
walcast setup      Create publication and replication slot (idempotent)
walcast serve      Run the sink daemon (needs at least one sink plugin)
walcast status     Show publication, slot, and retained-WAL lag
walcast teardown   Drop slot and publication (asks for confirmation)
walcast --version  Print version
```

## Flags

| Flag                   | Applies to            | Description                                                                                                                  |
| ---------------------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `--db <url>`           | all                   | Postgres connection string. Fallback: `DATABASE_URL` (for `serve`, the full [config resolution](/reference/config) applies). |
| `--config <path>`      | `serve`               | Daemon config file. Default `walcast.config.json`.                                                                          |
| `--publication <name>` | setup/status/teardown | Publication name. Default `walcast`.                                                                                        |
| `--slot <name>`        | setup/status/teardown | Slot name. Default `walcast`.                                                                                               |
| `--tables <a,b>`       | `setup`               | Limit the publication to these tables (comma-separated). Default: `FOR ALL TABLES`.                                          |
| `--yes`                | `teardown`            | Skip the confirmation prompt.                                                                                                |
| `--help`, `-h`         | all                   | Usage.                                                                                                                       |

For `serve`, publication/slot come from the config file or `WALCAST_PUBLICATION` / `WALCAST_SLOT` — not from flags.

## `walcast setup`

Verifies `wal_level = logical` (fails with instructions otherwise), creates the publication and the `pgoutput` logical replication slot if missing. Idempotent — never drops or replaces anything. Requires a role with `REPLICATION` (or superuser).

```
$ walcast setup --db postgres://localhost/mydb
publication 'walcast': ready (all tables)
slot 'walcast': ready (confirmed_flush 0/1A2B3C8)
```

## `walcast serve`

Loads sinks from the config, runs setup (idempotent), starts the engine and the admin server, prints the dashboard URL (with the generated token when none is pinned). Runs until `SIGINT`/`SIGTERM`, both of which shut down cleanly: engine stops, sinks close, final flushed position reported to Postgres. Exits non-zero with an instructive message when no sinks are configured. See the [daemon quickstart](/guide/quickstart-daemon).

## `walcast status`

Prints the setup status as JSON — the same shape as [`Walcast#status()`](/reference/config#library-mode-options) returns:

```json
{
  "walLevel": "logical",
  "publication": { "exists": true, "allTables": true },
  "slot": {
    "exists": true,
    "active": true,
    "restartLsn": "0/1A2B000",
    "confirmedFlushLsn": "0/1A2B3C8",
    "retainedWalBytes": 968
  }
}
```

`retainedWalBytes` is the disk-growth number — see [Monitoring](/guide/monitoring).

## `walcast teardown`

```
$ walcast teardown
Drop slot 'walcast' and publication 'walcast'? Undelivered changes are lost permanently. [y/N]
```

Drops the slot (releasing retained WAL) and the publication. Destructive — undelivered changes are gone for good — hence the prompt; `--yes` for scripts. Run this whenever you stop using walcast on a database: an orphaned slot retains WAL forever and will fill the disk.
