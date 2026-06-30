import { Kafka, type Admin, type KafkaConfig, type Message, type Producer } from 'kafkajs'
import {
  compareEventIds,
  type ChangeEvent,
  type Sink,
  type SinkContext,
  type SinkFactory,
} from '@walcast/plugin-kit'

export interface KafkaSinkConfig {
  brokers: string[]
  clientId?: string
  /** Topic strategy: `${topicPrefix}.${schema}.${table}`. Default 'walcast'. */
  topicPrefix?: string
  /**
   * Exactly-once into Kafka (default true): transactional producer with a
   * fixed transactional.id, checkpoints written in the same transaction as
   * the data. `false` falls back to a plain idempotent producer —
   * at-least-once, consumers deduplicate on the event id.
   */
  eos?: boolean
  /** Compacted topic holding `{ sinkId → last delivered event }`. */
  checkpointTopic?: string
  /**
   * Message keys: `{ "schema.table": ["id"] }` uses those columns from the
   * row image (per-key ordering in partitioned topics). Unlisted tables key
   * by event id.
   */
  keyColumns?: Record<string, string[]>
  /**
   * Kafka transactional.id. Default `walcast-<sinkId>` — keep it stable per
   * sink instance: it is what fences a zombie predecessor after a crash.
   */
  transactionalId?: string
  transactionTimeoutMs?: number
  /** Passed through to kafkajs. */
  ssl?: KafkaConfig['ssl']
  sasl?: KafkaConfig['sasl']
}

interface Checkpoint {
  lastEventId: string
  lastLsn: string
}

/**
 * Exactly-once into Kafka. Why this works (both crash windows):
 *
 * 1. Crash **before** the transaction commits — the broker aborts it, none
 *    of its records become visible to `read_committed` consumers, and the
 *    engine redelivers the batch. No duplicates.
 * 2. Crash **after** commit but before the engine records the ack — the
 *    engine redelivers, but the committed transaction included a checkpoint
 *    record (`sinkId → last event id`) in the compacted checkpoint topic.
 *    On startup the sink reads it back and skips everything at or below it.
 *    No duplicates.
 *
 * The fixed transactional.id additionally fences zombie producers: a
 * restarted sink bumps the producer epoch, and any in-flight transaction
 * from the dead process is aborted by the broker.
 */
class KafkaSink implements Sink {
  readonly name = 'kafka'
  readonly durability = 'durable' as const

  private cfg: Required<Pick<KafkaSinkConfig, 'topicPrefix' | 'eos' | 'checkpointTopic'>> &
    KafkaSinkConfig
  private kafka!: Kafka
  private producer!: Producer
  private admin!: Admin
  private ctx!: SinkContext
  private knownTopics = new Set<string>()
  private checkpointId: string | null = null

  constructor(config: Record<string, unknown>) {
    const brokers = config.brokers
    if (!Array.isArray(brokers) || brokers.length === 0) {
      throw new Error('@walcast/sink-kafka: config.brokers must be a non-empty array')
    }
    this.cfg = {
      topicPrefix: 'walcast',
      eos: true,
      checkpointTopic: '__walcast_checkpoints',
      ...(config as unknown as KafkaSinkConfig),
    }
  }

  async init(ctx: SinkContext): Promise<void> {
    this.ctx = ctx
    this.kafka = new Kafka({
      clientId: this.cfg.clientId ?? `walcast-${ctx.sinkId}`,
      brokers: this.cfg.brokers,
      ...(this.cfg.ssl !== undefined ? { ssl: this.cfg.ssl } : {}),
      ...(this.cfg.sasl !== undefined ? { sasl: this.cfg.sasl } : {}),
      logLevel: 1, // errors only; walcast has its own logger
    })
    this.admin = this.kafka.admin()
    await this.admin.connect()

    if (this.cfg.eos) {
      await this.ensureTopic(this.cfg.checkpointTopic, { 'cleanup.policy': 'compact' })
      this.checkpointId = (await this.readCheckpoint())?.lastEventId ?? null
      this.producer = this.kafka.producer({
        transactionalId: this.cfg.transactionalId ?? `walcast-${ctx.sinkId}`,
        idempotent: true,
        maxInFlightRequests: 1,
        ...(this.cfg.transactionTimeoutMs !== undefined
          ? { transactionTimeout: this.cfg.transactionTimeoutMs }
          : {}),
      })
    } else {
      this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
    }
    await this.producer.connect()
    ctx.logger.info('kafka sink ready', {
      brokers: this.cfg.brokers,
      eos: this.cfg.eos,
      checkpoint: this.checkpointId,
      engineResumeLsn: ctx.resumeLsn,
    })
  }

  async deliver(batch: ChangeEvent[]): Promise<void> {
    // Redeliveries at or below the transactional checkpoint were already
    // committed to Kafka in a previous life — this is the dedupe that makes
    // exactly-once hold across crash recovery.
    const fresh = this.checkpointId
      ? batch.filter((e) => compareEventIds(e.id, this.checkpointId!) > 0)
      : batch
    if (fresh.length === 0) return

    const byTopic = new Map<string, Message[]>()
    for (const event of fresh) {
      const topic = `${this.cfg.topicPrefix}.${event.schema}.${event.table}`
      let messages = byTopic.get(topic)
      if (!messages) byTopic.set(topic, (messages = []))
      messages.push({
        key: this.keyFor(event),
        value: JSON.stringify(event),
        headers: {
          id: event.id,
          lsn: event.lsn,
          commit_lsn: event.commit_lsn,
          op: event.op,
        },
      })
    }
    for (const topic of byTopic.keys()) await this.ensureTopic(topic)

    const last = fresh[fresh.length - 1]!
    if (!this.cfg.eos) {
      for (const [topic, messages] of byTopic) await this.producer.send({ topic, messages })
      this.checkpointId = last.id
      return
    }

    const txn = await this.openTransaction()
    try {
      for (const [topic, messages] of byTopic) await txn.send({ topic, messages })
      const checkpoint: Checkpoint = { lastEventId: last.id, lastLsn: last.commit_lsn }
      await txn.send({
        topic: this.cfg.checkpointTopic,
        messages: [{ key: this.ctx.sinkId, value: JSON.stringify(checkpoint) }],
      })
      await txn.commit()
    } catch (err) {
      await txn.abort().catch(() => {
        // The broker may have already resolved the transaction either way.
      })
      // A commit failure is ambiguous: the broker may have durably committed
      // before the response was lost. Because the checkpoint record travels
      // in the same transaction as the data, re-reading the checkpoint topic
      // resolves the ambiguity — if the commit landed, the checkpoint now
      // points at this batch and the engine's retry will be filtered out
      // instead of double-producing.
      try {
        const recovered = await this.readCheckpoint()
        if (
          recovered &&
          (!this.checkpointId || compareEventIds(recovered.lastEventId, this.checkpointId) > 0)
        ) {
          this.checkpointId = recovered.lastEventId
        }
      } catch {
        // Unreachable broker; the engine retries and dedupe re-resolves then.
      }
      throw err
    }
    this.checkpointId = last.id
  }

  async close(): Promise<void> {
    await this.producer?.disconnect().catch(() => {})
    await this.admin?.disconnect().catch(() => {})
  }

  /**
   * Opening a transaction right after (re)connecting can race the broker's
   * transaction coordinator (CONCURRENT_TRANSACTIONS while the previous
   * epoch's state settles). Those errors are marked retriable — do so here
   * instead of failing the batch out to the engine.
   */
  private async openTransaction() {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.producer.transaction()
      } catch (err) {
        if (!isRetriable(err) || attempt >= 12) throw err
        await new Promise((r) => setTimeout(r, Math.min(2_000, 250 * attempt)))
      }
    }
  }

  private keyFor(event: ChangeEvent): string {
    const cols = this.cfg.keyColumns?.[`${event.schema}.${event.table}`]
    const row = event.after ?? event.before
    if (cols && row) {
      const parts = cols.map((c) => {
        const v = row[c]
        if (v === null || v === undefined) return ''
        if (typeof v === 'object') return JSON.stringify(v)
        // eslint-disable-next-line @typescript-eslint/no-base-to-string -- objects handled above
        return String(v)
      })
      if (parts.some((p) => p !== '')) return parts.join('|')
    }
    return event.id
  }

  private async ensureTopic(topic: string, configEntries?: Record<string, string>): Promise<void> {
    if (this.knownTopics.has(topic)) return
    try {
      await this.admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: 1,
            configEntries: configEntries
              ? Object.entries(configEntries).map(([name, value]) => ({ name, value }))
              : [],
          },
        ],
        waitForLeaders: true,
      })
    } catch (err) {
      // Racing another instance is fine; anything else should surface.
      if (!/already exists/i.test(err instanceof Error ? err.message : '')) throw err
    }
    this.knownTopics.add(topic)
  }

  /** Latest checkpoint for this sinkId from the compacted topic. */
  private async readCheckpoint(): Promise<Checkpoint | null> {
    const offsets = await this.admin.fetchTopicOffsets(this.cfg.checkpointTopic)
    const pending = new Map<number, bigint>()
    for (const p of offsets) {
      if (BigInt(p.high) > BigInt(p.low)) pending.set(p.partition, BigInt(p.high))
    }
    if (pending.size === 0) return null

    const consumer = this.kafka.consumer({
      groupId: `walcast-checkpoint-reader-${this.ctx.sinkId}-${Date.now()}`,
    })
    await consumer.connect()
    await consumer.subscribe({ topic: this.cfg.checkpointTopic, fromBeginning: true })

    let latest: { offset: bigint; checkpoint: Checkpoint } | null = null
    try {
      await new Promise<void>((resolve, reject) => {
        const guard = setTimeout(
          () => reject(new Error('timed out reading the checkpoint topic')),
          30_000,
        )
        // Transaction control markers occupy tail offsets and are never
        // delivered, so "read up to the high watermark" can't be exact.
        // Fallback: 2s of silence after the FIRST delivered record. The idle
        // timer must not run before delivery starts — group joins alone
        // routinely take longer than any idle window
        // (group.initial.rebalance.delay.ms defaults to 3s), and resolving
        // early would return a stale checkpoint and silently break
        // exactly-once on redelivery. If nothing arrives within 10s of the
        // group join, the watermark gap is all control markers — done.
        let idle: NodeJS.Timeout | undefined
        const done = () => {
          clearTimeout(guard)
          clearTimeout(idle)
          resolve()
        }
        const armIdle = (ms: number) => {
          clearTimeout(idle)
          idle = setTimeout(done, ms)
        }
        let sawMessage = false
        consumer.on(consumer.events.GROUP_JOIN, () => {
          if (!sawMessage) armIdle(10_000)
        })
        consumer
          .run({
            eachMessage: ({ partition, message }) => {
              sawMessage = true
              armIdle(2_000)
              const offset = BigInt(message.offset)
              if (
                message.key?.toString() === this.ctx.sinkId &&
                message.value &&
                (!latest || offset > latest.offset)
              ) {
                latest = { offset, checkpoint: JSON.parse(message.value.toString()) as Checkpoint }
              }
              const high = pending.get(partition)
              // +2: the last offset is the transaction's control record.
              if (high !== undefined && offset + 2n >= high) {
                pending.delete(partition)
                if (pending.size === 0) done()
              }
              return Promise.resolve()
            },
          })
          .catch(reject)
      })
    } finally {
      await consumer.disconnect().catch(() => {})
    }
    return latest ? (latest as { checkpoint: Checkpoint }).checkpoint : null
  }
}

/** kafkajs wraps retriable causes in non-retriable retry-exceeded errors. */
function isRetriable(err: unknown): boolean {
  const e = err as { retriable?: boolean; originalError?: unknown; message?: string }
  if (e.retriable) return true
  if (e.originalError && isRetriable(e.originalError)) return true
  return /CONCURRENT_TRANSACTIONS|coordinator/i.test(e.message ?? '')
}

const factory: SinkFactory = (config) => new KafkaSink(config)
export default factory
