# Kafka sink

`@walcast/sink-kafka` — durable delivery into Kafka, **exactly-once into the topic** by default.

```bash
npm install @walcast/sink-kafka
```

```json
{
  "sinks": [
    {
      "use": "@walcast/sink-kafka",
      "config": {
        "brokers": ["localhost:9092"],
        "keyColumns": { "public.orders": ["id"] }
      }
    }
  ]
}
```

## Config

| Key                    | Type                       | Default                    | Description                                                                                                                                                                                       |
| ---------------------- | -------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `brokers`              | `string[]`                 | required                   | Kafka bootstrap brokers.                                                                                                                                                                          |
| `clientId`             | `string`                   | `walcast-<sinkId>`        | kafkajs client id.                                                                                                                                                                                |
| `topicPrefix`          | `string`                   | `'walcast'`               | Events go to `${topicPrefix}.${schema}.${table}`, e.g. `walcast.public.orders`. Topics are auto-created (1 partition).                                                                           |
| `eos`                  | `boolean`                  | `true`                     | Exactly-once into Kafka: transactional producer + checkpoint record in the same transaction. `false` falls back to a plain idempotent producer — at-least-once; consumers dedupe on the event id. |
| `checkpointTopic`      | `string`                   | `'__walcast_checkpoints'` | Compacted topic holding `{ sinkId → last delivered event }`.                                                                                                                                      |
| `keyColumns`           | `Record<string, string[]>` | —                          | Message keys per table: `{ "schema.table": ["id"] }` joins those columns from the row image with `\|` (per-key ordering in partitioned topics). Unlisted tables key by event id.                  |
| `transactionalId`      | `string`                   | `walcast-<sinkId>`        | Kafka `transactional.id`. Keep it stable per sink instance — it is what fences a zombie predecessor after a crash.                                                                                |
| `transactionTimeoutMs` | `number`                   | kafkajs default            | Transaction timeout passed to the producer.                                                                                                                                                       |
| `ssl`, `sasl`          |                            | —                          | Passed through to kafkajs.                                                                                                                                                                        |

## Message shape

Value: the [change event](/reference/event-schema) as JSON. Headers: `id`, `lsn`, `commit_lsn`, `op`.

## How exactly-once works (and its crash windows)

Each batch is one Kafka transaction: every event's record, plus a checkpoint record (`sinkId → last event id`) to the compacted checkpoint topic, then commit. The full walkthrough of both crash windows — crash before commit (aborted, invisible, redelivered, no dupes) and crash after commit but before the engine ack (redelivered, but skipped via the checkpoint read back at startup) — is in [Delivery guarantees](/guide/delivery-guarantees#kafka-exactly-once-into-the-topic).

What you must do on the consuming side:

- **Use `isolation.level=read_committed`.** Otherwise you read aborted transactions and the guarantee evaporates. (kafkajs consumers default to read-committed; the Java client does not.)
- **Don't reuse the `transactionalId` across different sink instances** pointing at different data — it must be stable per instance, unique across instances.

On startup with `eos: true` the sink reads the checkpoint topic back (it tolerates transaction control markers occupying tail offsets) and filters redelivered events at or below the checkpoint before producing. Redelivery from the engine is therefore invisible in the topic.

## Operational notes

- Client: **kafkajs** (pure JS) — no native build step for installers; transactions are all walcast needs from the client.
- Auto-created topics get 1 partition. If you need more partitions, create the topic yourself first — and set `keyColumns` so per-entity ordering survives partitioning. Global ordering across partitions does not exist in Kafka; per-key ordering does.
- Opening a transaction right after a reconnect can race the broker's transaction coordinator (`CONCURRENT_TRANSACTIONS`); the sink retries those internally instead of failing the batch out to the engine.
- Single-broker dev clusters: the broker's own transaction coordinator dials the _advertised_ listener. If your container only advertises a host-mapped address, transactional produces die with `NOT_ENOUGH_REPLICAS` — advertise an internal listener for the broker itself and an external one for the host.
