# @walcast/sink-sse

Live Server-Sent Events tail for [walcast](https://github.com/ManasMadan/walcast).
Mounts `GET /plugins/<sinkId>/events` on the daemon; powers the dashboard's
live inspector. Node stdlib only.

```bash
npm install walcast @walcast/sink-sse
```

```jsonc
// walcast.config.json
{ "sinks": [{ "use": "@walcast/sink-sse" }] }
```

```bash
curl -N "http://127.0.0.1:7717/plugins/sse/events?tables=orders&token=..."
```

**This sink is deliberately ephemeral** — the opposite of a durable sink:
no connected client, no delivery; a client that connects late missed what
came before; and it is excluded from the replication slot's min-LSN
computation, so a slow dashboard can never make Postgres retain WAL. If you
need events reliably, use a durable sink; if you need to _watch_, use this.

Heartbeat comments every 15s keep proxies from closing idle streams.

Docs: https://walcast.mmadan.in/guide/sinks/sse

## License

MIT
