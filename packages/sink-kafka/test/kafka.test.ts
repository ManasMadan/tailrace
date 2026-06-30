import { randomUUID } from 'node:crypto'
import { describe, expect, inject, it } from 'vitest'
import { Kafka, logLevel } from 'kafkajs'
import { makeMockContext, makeTestEvents, verifySink, type ChangeEvent } from '@walcast/plugin-kit'
import factory from '@/index'

const brokers = inject('brokers')
const kafka = brokers.length
  ? new Kafka({ clientId: 'sink-kafka-tests', brokers, logLevel: logLevel.NOTHING })
  : null

/** Read a whole topic with read_committed — exactly what a consumer sees. */
async function readTopic(topic: string, timeoutMs = 20_000): Promise<ChangeEvent[]> {
  const admin = kafka!.admin()
  await admin.connect()
  const offsets = await admin.fetchTopicOffsets(topic).catch(() => [])
  await admin.disconnect()
  const pending = new Map<number, bigint>()
  for (const p of offsets)
    if (BigInt(p.high) > BigInt(p.low)) pending.set(p.partition, BigInt(p.high))
  if (pending.size === 0) return []

  const consumer = kafka!.consumer({
    groupId: `read-${randomUUID()}`,
    readUncommitted: false,
  })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  const out: Array<{ offset: bigint; event: ChangeEvent }> = []
  try {
    await new Promise<void>((resolve, reject) => {
      const guard = setTimeout(() => resolve(), timeoutMs) // aborted txns leave gaps; settle for what we have
      consumer
        .run({
          eachMessage: ({ partition, message }) => {
            if (message.value) {
              out.push({
                offset: BigInt(message.offset),
                event: JSON.parse(message.value.toString()) as ChangeEvent,
              })
            }
            const high = pending.get(partition)
            if (high !== undefined && BigInt(message.offset) + 2n >= high) {
              // +2: the transaction control record occupies the last offset
              pending.delete(partition)
              if (pending.size === 0) {
                clearTimeout(guard)
                resolve()
              }
            }
            return Promise.resolve()
          },
        })
        .catch(reject)
    })
  } finally {
    await consumer.disconnect().catch(() => {})
  }
  return out.sort((a, b) => (a.offset < b.offset ? -1 : 1)).map((o) => o.event)
}

describe.skipIf(!brokers.length)('@walcast/sink-kafka (live broker)', () => {
  it('passes the conformance harness with exactly-once expectations', async () => {
    const prefix = `conf${randomUUID().slice(0, 8)}`
    await verifySink(factory, {
      config: {
        brokers,
        topicPrefix: prefix,
        transactionalId: `txn-${prefix}`,
        transactionTimeoutMs: 5000,
        checkpointTopic: `${prefix}.checkpoints`,
      },
      expectDedupe: true,
      collect: () => readTopic(`${prefix}.public.conformance`),
    })
  }, 60_000)

  it('resumes from the transactional checkpoint and skips redelivered events', async () => {
    const prefix = `resume${randomUUID().slice(0, 8)}`
    const config = {
      brokers,
      topicPrefix: prefix,
      transactionalId: `txn-${prefix}`,
      transactionTimeoutMs: 5000,
      checkpointTopic: `${prefix}.checkpoints`,
    }
    const events = makeTestEvents(10)

    const first = factory(config)
    await first.init(makeMockContext(config, 'kafka-resume'))
    await first.deliver(events.slice(0, 6))
    await first.close()

    // New process, same sink id: crash-recovery redelivery of everything.
    const second = factory(config)
    await second.init(makeMockContext(config, 'kafka-resume'))
    await second.deliver(events) // 0..5 must be skipped, 6..9 written
    await second.close()

    const seen = await readTopic(`${prefix}.public.conformance`)
    expect(seen.map((e) => e.id)).toEqual(events.map((e) => e.id))
  }, 60_000)

  it('eos:false falls back to at-least-once (duplicates possible, none lost)', async () => {
    const prefix = `alo${randomUUID().slice(0, 8)}`
    const config = { brokers, topicPrefix: prefix, eos: false }
    const events = makeTestEvents(5)

    const sink = factory(config)
    await sink.init(makeMockContext(config, 'kafka-alo'))
    await sink.deliver(events)
    await sink.close()

    // A fresh instance has no transactional checkpoint — redelivery duplicates.
    const again = factory(config)
    await again.init(makeMockContext(config, 'kafka-alo'))
    await again.deliver(events.slice(3))
    await again.close()

    const seen = await readTopic(`${prefix}.public.conformance`)
    const ids = seen.map((e) => e.id)
    for (const e of events) expect(ids).toContain(e.id) // nothing lost
    expect(ids.length).toBeGreaterThan(events.length) // dupes present
  }, 60_000)

  it('rejects config without brokers', () => {
    expect(() => factory({})).toThrow(/brokers/)
  })
})
