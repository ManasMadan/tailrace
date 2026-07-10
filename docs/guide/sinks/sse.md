# SSE sink

`@walcast/sink-sse` — a live tail over Server-Sent Events, for dashboards and debugging.

```bash
npm install @walcast/sink-sse
```

```json
{
  "sinks": [{ "use": "@walcast/sink-sse", "config": {} }]
}
```

## Config

| Key           | Type     | Default | Description                                                                                   |
| ------------- | -------- | ------- | --------------------------------------------------------------------------------------------- |
| `heartbeatMs` | `number` | `15000` | Interval for `: heartbeat` comment frames, which keep idle connections alive through proxies. |

## Ephemeral, deliberately — read this before relying on it

This sink is the deliberate opposite of a durable sink. The two semantics side by side:

|                       | Durable (webhook/Kafka/gRPC)                                 | Ephemeral (SSE)                          |
| --------------------- | ------------------------------------------------------------ | ---------------------------------------- |
| No receiver connected | events queue; queue full ⇒ engine backpressures Postgres     | events are **not delivered at all**      |
| Delivery failure      | retry with backoff, pause after max attempts                 | logged and **dropped**, never retried    |
| Slow consumer         | holds the replication slot; WAL retained until it catches up | full queue **drops** events              |
| Slot / WAL retention  | participates in the slot's min-LSN — can hold WAL            | **excluded** — can never hold WAL back   |
| After a crash         | undelivered events redelivered from the slot                 | missed events are **gone** for this sink |

If you need events reliably, use a durable sink. If you need to _watch_, this is the one. A live UI that renders current state from the database on load and uses SSE for incremental updates is the intended pattern — a missed frame costs a refresh, not data.

## Connecting

The sink mounts `GET /plugins/<sinkId>/events` on the daemon's server (sinkId defaults to `sink-sse`). It sits behind the daemon's bearer auth; browsers' `EventSource` can't set headers, so pass `?token=`:

```js
const es = new EventSource(
  'http://127.0.0.1:7717/plugins/sink-sse/events?token=' + TOKEN + '&tables=users,public.orders',
)
es.addEventListener('change', (e) => {
  const event = JSON.parse(e.data) // a ChangeEvent
  console.log(event.op, event.table, event.after)
})
```

- `?tables=a,b` filters to those tables — bare names (`users`) or schema-qualified (`public.users`).
- Each frame is `event: change` with `id:` set to the event id and `data:` the JSON [change event](/reference/event-schema).
- Heartbeat comments arrive every `heartbeatMs`; the first frame is a comment reminding you the stream is best-effort.
- On daemon shutdown clients get a `: server shutting down` comment and the connection ends.

No client connected means `deliver()` returns immediately — no listener, no delivery.
