// Library mode: no daemon, no plugins — your code is the sink.
import { Walcast } from 'walcast'

const tr = new Walcast({
  connection: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/postgres',
  publication: 'example_basic',
  slot: 'example_basic',
})

// Creates the publication and replication slot if missing. Idempotent.
await tr.setup()

// stop() ends the changes() iteration; the loop below falls through.
process.on('SIGINT', () => void tr.stop())

console.log('waiting for changes — INSERT/UPDATE/DELETE something (Ctrl+C to stop)')
for await (const event of tr.changes()) {
  const row = event.after ?? event.before
  console.log(`${event.op.padEnd(6)} ${event.schema}.${event.table} ${event.id}`, row)

  // Ack once the event is durably processed. Acks are cumulative (like a
  // Kafka offset commit): anything unacked at a crash is redelivered on
  // restart with the same event.id.
  tr.ack(event)
}
console.log('stopped')
