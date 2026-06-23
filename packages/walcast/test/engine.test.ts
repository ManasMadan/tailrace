import { afterAll, beforeAll, describe, expect, inject, it } from 'vitest'
import pg from 'pg'
import type { ChangeEvent, Sink } from '@walcast/plugin-kit'
import { SinkEngine } from '@/engine/engine'
import { createLogger } from '@/logger'
import { parseLsn } from '@/lsn'
import { Walcast } from '@/walcast'

const dsn = inject('dsn')

async function until(cond: () => boolean, what: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((r) => setTimeout(r, 50))
  }
}

interface MemorySink extends Sink {
  received: ChangeEvent[]
  failures: number
  /** >0 = fail the next N deliver calls; -1 = fail forever until set to 0. */
  failNext: number
  /** When set, deliver blocks until the promise resolves. */
  gate: Promise<void> | null
}

function memorySink(name: string, durability: 'durable' | 'ephemeral' = 'durable'): MemorySink {
  const sink: MemorySink = {
    name,
    durability,
    received: [],
    failures: 0,
    failNext: 0,
    gate: null,
    async init() {},
    async deliver(batch) {
      if (sink.gate) await sink.gate
      if (sink.failNext !== 0) {
        if (sink.failNext > 0) sink.failNext--
        sink.failures++
        throw new Error(`${name}: injected failure`)
      }
      sink.received.push(...batch)
    },
    async close() {},
  }
  return sink
}

const quietLogger = createLogger('error')

describe.skipIf(!dsn)('SinkEngine (live Postgres)', () => {
  let db: pg.Client

  beforeAll(async () => {
    db = new pg.Client({ connectionString: dsn })
    await db.connect()
  })

  afterAll(async () => {
    await db.end()
  })

  async function fixture(name: string, sinks: Array<{ id: string; sink: Sink }>, engineOpts = {}) {
    await db.query(`DROP TABLE IF EXISTS ${name}`)
    await db.query(`CREATE TABLE ${name} (id serial PRIMARY KEY, n int)`)
    const tr = new Walcast({
      connection: dsn,
      publication: `pub_${name}`,
      slot: `slot_${name}`,
      tables: [name],
      statusIntervalMs: 300,
    })
    await tr.teardown().catch(() => {})
    await db.query(`DELETE FROM walcast.sinks`).catch(() => {})
    await tr.setup()
    const engine = new SinkEngine({
      walcast: tr,
      connection: dsn,
      sinks: sinks.map((s) => ({ ...s, config: {} })),
      logger: quietLogger,
      lingerMs: 5,
      backoffBaseMs: 10,
      backoffCapMs: 50,
      ...engineOpts,
    })
    await engine.start()
    return { tr, engine }
  }

  it('delivers ordered batches and checkpoints the sink', async () => {
    const sink = memorySink('mem')
    const { tr, engine } = await fixture('e_order', [{ id: 'mem', sink }])
    for (let i = 0; i < 25; i++) await db.query(`INSERT INTO e_order (n) VALUES ($1)`, [i])

    await until(() => sink.received.length === 25, '25 events')
    expect(sink.received.map((e) => e.after?.n)).toEqual([...Array(25).keys()])

    const checkpoints = await engine.checkpoints.list()
    const cp = checkpoints.find((c) => c.sinkId === 'mem')
    expect(cp?.ackedLsn).toBe(sink.received[24]!.commit_lsn)
    expect(cp?.ackedEventId).toBe(sink.received[24]!.id)

    await engine.stop()
    await tr.teardown()
  })

  it('retries a failing durable sink with backoff, then succeeds', async () => {
    const sink = memorySink('flaky')
    sink.failNext = 3
    const { tr, engine } = await fixture('e_retry', [{ id: 'flaky', sink }])
    await db.query(`INSERT INTO e_retry (n) VALUES (1)`)

    await until(() => sink.received.length === 1, 'delivery after retries')
    expect(sink.failures).toBe(3)

    await engine.stop()
    await tr.teardown()
  })

  it('pauses a durable sink after max attempts, resumes with the same batch', async () => {
    const sink = memorySink('brittle')
    sink.failNext = -1
    const { tr, engine } = await fixture('e_pause', [{ id: 'brittle', sink }], { maxAttempts: 2 })
    await db.query(`INSERT INTO e_pause (n) VALUES (42)`)

    await until(() => engine.stats().sinks[0]!.status === 'paused', 'sink to pause')
    expect(engine.stats().sinks[0]!.lastError).toMatch(/injected failure/)
    expect(sink.received.length).toBe(0)

    sink.failNext = 0 // receiver is healthy again
    await engine.resume('brittle')
    await until(() => sink.received.length === 1, 'delivery after resume')
    expect(sink.received[0]!.after?.n).toBe(42)
    expect(engine.stats().sinks[0]!.status).toBe('running')

    await engine.stop()
    await tr.teardown()
  })

  it('an operator pause actually stops delivery and resume picks it back up', async () => {
    const sink = memorySink('op')
    const { tr, engine } = await fixture('e_oppause', [{ id: 'op', sink }])
    await db.query(`INSERT INTO e_oppause (n) VALUES (1)`)
    await until(() => sink.received.length === 1, 'first delivery')

    await engine.pause('op')
    await db.query(`INSERT INTO e_oppause (n) VALUES (2)`)
    await new Promise((r) => setTimeout(r, 600))
    expect(sink.received.length).toBe(1) // nothing delivered while paused

    await engine.resume('op')
    await until(() => sink.received.length === 2, 'delivery after resume')

    await engine.stop()
    await tr.teardown()
  })

  it('a failing ephemeral sink never blocks durable delivery or the slot', async () => {
    const good = memorySink('good')
    const bad = memorySink('bad-sse', 'ephemeral')
    bad.failNext = -1
    const { tr, engine } = await fixture('e_ephemeral', [
      { id: 'good', sink: good },
      { id: 'bad-sse', sink: bad },
    ])
    for (let i = 0; i < 10; i++) await db.query(`INSERT INTO e_ephemeral (n) VALUES ($1)`, [i])

    await until(() => good.received.length === 10, 'durable delivery')
    // Durable checkpoint advanced although the ephemeral sink failed everything.
    const stats = engine.stats()
    expect(stats.sinks.find((s) => s.id === 'good')?.ackedLsn).toBeTruthy()
    expect(stats.sinks.find((s) => s.id === 'bad-sse')?.deliveredCount).toBe(0)
    expect(bad.failures).toBeGreaterThan(0)

    await engine.stop()
    await tr.teardown()
  })

  it('the slot advances only to the minimum acked LSN across durable sinks', async () => {
    const fast = memorySink('fast')
    const slow = memorySink('slow')
    let release!: () => void
    slow.gate = new Promise((r) => (release = r))

    const { tr, engine } = await fixture('e_min', [
      { id: 'fast', sink: fast },
      { id: 'slow', sink: slow },
    ])
    for (let i = 0; i < 5; i++) await db.query(`INSERT INTO e_min (n) VALUES ($1)`, [i])

    await until(() => fast.received.length === 5, 'fast sink delivery')
    await new Promise((r) => setTimeout(r, 800)) // let any (wrong) acks land
    // The slow sink has delivered nothing: the slot must not advance past
    // the first event, even though the fast sink is done.
    const flushed = tr.flushedLsn
    const firstCommit = fast.received[0]!.commit_lsn
    expect(flushed === null || parseLsn(flushed) < parseLsn(firstCommit)).toBe(true)

    release()
    slow.gate = null
    await until(() => slow.received.length === 5, 'slow sink catches up')
    await until(
      () =>
        tr.flushedLsn !== null && parseLsn(tr.flushedLsn) >= parseLsn(fast.received[4]!.commit_lsn),
      'slot advances past the last commit once both sinks delivered',
    )

    await engine.stop()
    await tr.teardown()
  })
})
