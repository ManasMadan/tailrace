# @walcast/ui

The walcast dashboard: a Vite + React + Tailwind SPA compiled to static
assets inside the `walcast` npm package (`dist/ui`) and served by the
daemon at `/ui`. It is control plane only — it observes and manages the
engine and transports no events.

Never published to npm; it ships as files, not a dependency.

## Pages

- **Overview** — the WAL flow rail (slot `restart_lsn` → per-sink acked
  positions → head), slot lag with a warning threshold, events/sec
  sparkline
- **Sinks** — per-sink status, acked LSN, queue depth, pause/resume,
  paused-with-last-error detail
- **Live inspector** — event tail over `@walcast/sink-sse` when installed;
  an install hint when not
- **Setup** — publication/slot/wal_level status and the teardown story

## Development

```bash
pnpm --filter @walcast/ui dev     # Vite dev server, proxies /api + /plugins to :7717
pnpm --filter @walcast/ui build   # emits into packages/walcast/dist/ui
```

Run a daemon locally first so the proxy has something to talk to. The app
is fully self-contained (system fonts, hand-rolled SVG charts, no CDN) so
the dashboard works next to an air-gapped database.
