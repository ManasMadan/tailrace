// A minimal walcast gRPC consumer: implements walcast.v1.WalcastSink,
// prints every change, acks when done. Run it, then configure the daemon:
//
//   { "sinks": [{ "use": "@walcast/sink-grpc", "config": { "address": "localhost:50051" } }] }
//
// Contract reminders (see proto/walcast/v1/sink.proto):
// - return ok=true ONLY after you've durably processed the batch
// - batches can be redelivered; deduplicate on event.id
import { fileURLToPath } from 'node:url'
import grpc from '@grpc/grpc-js'
import protoLoader from '@grpc/proto-loader'

const PROTO = fileURLToPath(new URL('../../proto/walcast/v1/sink.proto', import.meta.url))
const PORT = process.env.PORT ?? '50051'

const definition = protoLoader.loadSync(PROTO, { keepCase: true, defaults: true })
const { walcast } = grpc.loadPackageDefinition(definition)

// Idempotency: remember processed event ids (use your database in real life).
const processed = new Set()

const server = new grpc.Server()
server.addService(walcast.v1.WalcastSink.service, {
  Deliver(call, callback) {
    try {
      for (const wire of call.request.events) {
        if (processed.has(wire.id)) continue // redelivery — already handled
        const before = wire.before_json ? JSON.parse(wire.before_json) : null
        const after = wire.after_json ? JSON.parse(wire.after_json) : null
        console.log(`${wire.op.padEnd(8)} ${wire.schema}.${wire.table} ${wire.id}`, after ?? before)
        processed.add(wire.id)
      }
      callback(null, { ok: true, message: '' })
    } catch (err) {
      // Anything not-ok makes walcast retry the batch with backoff.
      callback(null, { ok: false, message: String(err) })
    }
  },
})

server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) throw err
  console.log(`WalcastSink consumer listening on :${port}`)
})
