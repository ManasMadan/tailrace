# kafka-consumer

A kafkajs consumer for events produced by `@walcast/sink-kafka`. The sink names topics `${topicPrefix}.${schema}.${table}` — this example reads `walcast.public.orders`.

Why `readUncommitted: false` (read_committed) matters: the sink writes every batch inside a Kafka transaction, together with its own checkpoint record. If the sink crashes mid-batch, the broker aborts the transaction and none of its records ever become visible to a read_committed consumer — the engine then redelivers, and the committed checkpoint skips anything already written. That closes both crash windows, so this consumer sees each event exactly once and needs no deduplication. A read_uncommitted consumer would forfeit that and see records from aborted transactions.

(If the sink is configured with `"eos": false`, delivery into Kafka is at-least-once instead — then deduplicate on `event.id`, which is stable across redeliveries.)

## Prerequisites

A Kafka broker, and a walcast daemon producing into it. Matching `walcast.config.json` snippet:

```json
{
  "sinks": [
    {
      "use": "@walcast/sink-kafka",
      "config": { "brokers": ["localhost:9092"], "topicPrefix": "walcast" }
    }
  ]
}
```

## Run

```sh
export KAFKA_BROKERS=localhost:9092
npm start
```

Every committed change to `public.orders` prints.
