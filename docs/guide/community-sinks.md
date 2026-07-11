# Community sinks

The core ships zero sinks on purpose — the plugin contract is the product. If you've built a transport, we want it listed here.

## Good first plugins

Each of these is a genuinely useful afternoon project against the [Sink interface](/guide/writing-a-sink):

- **Discord / Slack notifier** _(ephemeral or durable)_ — post row changes to a channel. The simplest possible transport: one `fetch` in `deliver`.
- **NDJSON file sink** _(durable)_ — append events to a file, fsync per batch. The [tutorial](/guide/writing-a-sink) builds exactly this; productionize it with rotation and you have a poor-man's audit log.
- **Meilisearch / Typesense indexer** _(durable)_ — keep a search index in sync: upsert on insert/update, delete on delete. Naturally idempotent because documents are keyed by primary key.
- **Redis Streams** _(durable)_ — `XADD` each event; consumers get consumer-groups and acking for free. Use the event id as a dedupe key.
- **NATS / JetStream** _(durable)_ — publish per-table subjects (`walcast.public.users`); JetStream's `Nats-Msg-Id` header gives you server-side dedupe on the event id.
- **Webhook fan-out** _(durable)_ — one config, many receiver URLs, per-receiver checkpointing. A study in what the engine does and doesn't do for you.

Durability rule of thumb: if a missed event is a bug, declare `durable`; if it costs a refresh, `ephemeral`. The [contract](/guide/writing-a-sink#the-contract) has the details.

## Where to start

1. Read the 15-minute [tutorial](/guide/writing-a-sink) — it builds a complete working sink.
2. Start from the plugin template: `templates/plugin` in the repo (`https://github.com/ManasMadan/walcast`).
3. Depend only on `@walcast/plugin-kit` (types + conformance harness). Never import the core.
4. Develop against a local path first — `{ "use": "./my-sink.js" }` in `walcast.config.json` works without publishing.

## PR checklist for getting listed

Open a PR adding your sink to this page with:

- [ ] Published npm package (any scope; `walcast-sink-*` naming appreciated but not required) with a `README` documenting every config key and the durable/ephemeral semantics.
- [ ] Default export is a factory `(config) => Sink`; constructor validates config, does no I/O.
- [ ] `verifySink` from `@walcast/plugin-kit` passes in your CI, with a `collect` implementation that reads your real transport back (see [the harness](/guide/writing-a-sink#verify-it-with-the-conformance-harness)).
- [ ] Redelivery behavior stated explicitly in your README: dedupe (like the Kafka sink) or duplicates-possible (like the webhook sink) — either is fine, undocumented is not.
- [ ] A runnable example (a `examples/` dir or a snippet that works verbatim).
- [ ] License compatible with inclusion in a listing (any OSI license).

One-line listing format: package name, transport, durability, one honest sentence about semantics. We'll take it from there.
