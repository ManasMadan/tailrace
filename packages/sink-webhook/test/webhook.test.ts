import { createServer, type Server } from 'node:http'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { makeMockContext, makeTestEvents, verifySink, type ChangeEvent } from '@walcast/plugin-kit'
import factory, { verifySignature } from '@/index'

/** Capturing receiver: records batches, bodies, and headers. */
function receiver() {
  const events: ChangeEvent[] = []
  const signatures: Array<string | undefined> = []
  const bodies: string[] = []
  let failNext = 0
  const server = createServer((req, res) => {
    let body = ''
    req.on('data', (c: Buffer) => (body += c.toString()))
    req.on('end', () => {
      if (failNext > 0) {
        failNext--
        res.writeHead(503).end()
        return
      }
      bodies.push(body)
      signatures.push(req.headers['x-walcast-signature'] as string | undefined)
      events.push(...(JSON.parse(body) as ChangeEvent[]))
      res.writeHead(200).end('ok')
    })
  })
  return {
    server,
    events,
    signatures,
    bodies,
    failTimes: (n: number) => (failNext = n),
  }
}

describe('@walcast/sink-webhook', () => {
  let rx: ReturnType<typeof receiver>
  let server: Server
  let url: string

  beforeAll(async () => {
    rx = receiver()
    server = rx.server
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    const addr = server.address()
    url = `http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}/hook`
  })

  afterAll(() => new Promise<void>((r) => server.close(() => r())))

  it('passes the conformance harness', async () => {
    await verifySink(factory, {
      config: { url },
      collect: () => Promise.resolve([...rx.events]),
    })
  })

  it('signs bodies with HMAC-SHA256 and receivers can verify', async () => {
    rx.events.length = 0
    rx.bodies.length = 0
    rx.signatures.length = 0
    const sink = factory({ url, secret: 'shhh' })
    await sink.init(makeMockContext())
    await sink.deliver(makeTestEvents(3))
    await sink.close()

    expect(rx.signatures[0]).toMatch(/^sha256=[0-9a-f]{64}$/)
    expect(verifySignature(rx.bodies[0]!, 'shhh', rx.signatures[0]!)).toBe(true)
    expect(verifySignature(rx.bodies[0]!, 'wrong', rx.signatures[0]!)).toBe(false)
  })

  it('throws on non-2xx so the engine can retry', async () => {
    const sink = factory({ url })
    await sink.init(makeMockContext())
    rx.failTimes(1)
    await expect(sink.deliver(makeTestEvents(2))).rejects.toThrow(/503/)
    // Engine-style retry of the identical batch then succeeds.
    await sink.deliver(makeTestEvents(2))
    await sink.close()
  })

  it('rejects a config without a url', () => {
    expect(() => factory({})).toThrow(/config\.url/)
  })
})
