import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import grpc from '@grpc/grpc-js'
import protoLoader from '@grpc/proto-loader'
import { makeMockContext, makeTestEvents, verifySink, type ChangeEvent } from '@walcast/plugin-kit'
import factory, { PROTO_PATH } from '@/index'

/** In-process WalcastSink server, the same way a consumer would build one. */
function testServer() {
  const received: ChangeEvent[] = []
  let rejectNext = 0
  const definition = protoLoader.loadSync(PROTO_PATH, { keepCase: true, defaults: true })
  const pkg = grpc.loadPackageDefinition(definition) as never as {
    walcast: { v1: { WalcastSink: { service: grpc.ServiceDefinition } } }
  }
  const server = new grpc.Server()
  server.addService(pkg.walcast.v1.WalcastSink.service, {
    Deliver: (
      call: { request: { events: Array<Record<string, string>> } },
      callback: (err: null, ack: { ok: boolean; message: string }) => void,
    ) => {
      if (rejectNext > 0) {
        rejectNext--
        callback(null, { ok: false, message: 'injected rejection' })
        return
      }
      for (const w of call.request.events) {
        received.push({
          id: w.id!,
          lsn: w.lsn!,
          commit_lsn: w.commit_lsn!,
          commit_time: w.commit_time!,
          schema: w.schema!,
          table: w.table!,
          op: w.op as ChangeEvent['op'],
          before: w.before_json ? (JSON.parse(w.before_json) as Record<string, unknown>) : null,
          after: w.after_json ? (JSON.parse(w.after_json) as Record<string, unknown>) : null,
        })
      }
      callback(null, { ok: true, message: '' })
    },
  })
  return {
    received,
    rejectTimes: (n: number) => (rejectNext = n),
    listen: () =>
      new Promise<number>((resolve, reject) => {
        server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(), (err, port) =>
          err ? reject(err) : resolve(port),
        )
      }),
    stop: () => new Promise<void>((r) => server.tryShutdown(() => r())),
  }
}

describe('@walcast/sink-grpc', () => {
  let server: ReturnType<typeof testServer>
  let address: string

  beforeAll(async () => {
    server = testServer()
    address = `127.0.0.1:${await server.listen()}`
  })

  afterAll(() => server.stop())

  it('passes the conformance harness against a real gRPC server', async () => {
    await verifySink(factory, {
      config: { address },
      collect: () => Promise.resolve([...server.received]),
    })
  })

  it('round-trips row images through the JSON fields', async () => {
    server.received.length = 0
    const sink = factory({ address })
    await sink.init(makeMockContext())
    const events = makeTestEvents(4)
    await sink.deliver(events)
    await sink.close()
    expect(server.received.map((e) => ({ id: e.id, before: e.before, after: e.after }))).toEqual(
      events.map((e) => ({ id: e.id, before: e.before, after: e.after })),
    )
  })

  it('throws when the consumer acks ok=false, so the engine retries', async () => {
    const sink = factory({ address })
    await sink.init(makeMockContext())
    server.rejectTimes(1)
    await expect(sink.deliver(makeTestEvents(2))).rejects.toThrow(/injected rejection/)
    await sink.deliver(makeTestEvents(2)) // engine-style retry succeeds
    await sink.close()
  })

  it('throws on an unreachable server (deadline)', async () => {
    const sink = factory({ address: '127.0.0.1:1', deadlineMs: 800 })
    await sink.init(makeMockContext())
    await expect(sink.deliver(makeTestEvents(1))).rejects.toThrow()
    await sink.close()
  })

  it('rejects config without an address', () => {
    expect(() => factory({})).toThrow(/address/)
  })

  it('ships the exact proto published at the repo root', () => {
    const packaged = readFileSync(PROTO_PATH, 'utf8')
    const canonical = readFileSync(
      fileURLToPath(new URL('../../../proto/walcast/v1/sink.proto', import.meta.url)),
      'utf8',
    )
    expect(packaged).toBe(canonical)
  })
})
