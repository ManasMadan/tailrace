import { describe, expect, it } from 'vitest'
import { makeMockContext, makeTestEvents, verifySink } from '@/harness'
import type { ChangeEvent, Sink, SinkContext } from '@/types'

/** Reference in-memory sink: durable, dedupes by event id (EOS-style). */
function memorySink(received: ChangeEvent[], opts: { dedupe?: boolean } = {}): Sink {
  const seen = new Set<string>()
  return {
    name: 'memory',
    durability: 'durable',
    async init(_ctx: SinkContext) {},
    async deliver(batch) {
      for (const e of batch) {
        if (opts.dedupe && seen.has(e.id)) continue
        seen.add(e.id)
        received.push(e)
      }
    },
    async close() {},
  }
}

describe('verifySink', () => {
  it('passes a conforming at-least-once sink', async () => {
    const received: ChangeEvent[] = []
    await verifySink(() => memorySink(received), {
      collect: () => Promise.resolve(received),
    })
  })

  it('passes a deduplicating sink with expectDedupe', async () => {
    const received: ChangeEvent[] = []
    await verifySink(() => memorySink(received, { dedupe: true }), {
      collect: () => Promise.resolve(received),
      expectDedupe: true,
    })
  })

  it('fails a sink that reorders events', async () => {
    const received: ChangeEvent[] = []
    const sink = memorySink(received)
    const shuffling: Sink = {
      ...sink,
      deliver: (batch) => sink.deliver([...batch].reverse()),
    }
    await expect(
      verifySink(() => shuffling, { collect: () => Promise.resolve(received) }),
    ).rejects.toThrow(/order preserved/)
  })

  it('fails a sink that drops events', async () => {
    const received: ChangeEvent[] = []
    const sink = memorySink(received)
    const lossy: Sink = { ...sink, deliver: (batch) => sink.deliver(batch.slice(1)) }
    await expect(
      verifySink(() => lossy, { collect: () => Promise.resolve(received) }),
    ).rejects.toThrow(/every event/)
  })

  it('fails a duplicate-producing sink when expectDedupe is set', async () => {
    const received: ChangeEvent[] = []
    await expect(
      verifySink(() => memorySink(received), {
        collect: () => Promise.resolve(received),
        expectDedupe: true,
      }),
    ).rejects.toThrow(/exactly one copy/)
  })

  it('fails on invalid metadata', async () => {
    const received: ChangeEvent[] = []
    const bad = { ...memorySink(received), durability: 'sometimes' as 'durable' }
    await expect(verifySink(() => bad)).rejects.toThrow(/durability/)
  })

  it('mock context rejects duplicate route registration', () => {
    const ctx = makeMockContext()
    ctx.http.registerRoute('/events', () => {})
    expect(() => ctx.http.registerRoute('/events', () => {})).toThrow(/twice/)
    expect(() => ctx.http.registerRoute('no-slash', () => {})).toThrow(/start with/)
  })

  it('makeTestEvents produces stable deterministic ids', () => {
    const a = makeTestEvents(5)
    const b = makeTestEvents(5)
    expect(a).toEqual(b)
    expect(new Set(a.map((e) => e.id)).size).toBe(5)
  })
})
