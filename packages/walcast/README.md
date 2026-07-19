# walcast

Postgres change data capture for Node: a hand-written `pgoutput` decoder
speaking the streaming replication protocol, exposed as an async iterator
with explicit LSN acknowledgment — plus a plugin engine and daemon when you
want delivery managed for you.

**The core transports nothing.** Every event transport is a separately
installed `@walcast/*` plugin; library mode needs none of them.

```bash
npm install walcast
```

```ts
import { Walcast } from 'walcast'

const tr = new Walcast({ connection: process.env.DATABASE_URL! })
await tr.setup()

for await (const event of tr.changes()) {
  await handle(event) // at-least-once; event.id is stable across redelivery
  tr.ack(event) // the replication slot only advances past acked work
}
```

Daemon mode (`npx walcast serve`) runs sink plugins with ordering,
batching, retries, backpressure, and per-sink LSN checkpoints, and serves a
dashboard at `/ui`:

```bash
npm install @walcast/sink-webhook
npx walcast serve
```

Docs: https://walcast.mmadan.in ·
Repo: https://github.com/ManasMadan/walcast

## License

MIT
