import type pg from 'pg'
import type { ChangeEvent } from '@/events'
import { formatLsn, parseLsn, type Lsn } from '@/lsn'
import { PgoutputDecoder } from '@/pgoutput/decoder'
import { AsyncQueue } from '@/queue'
import { ReplicationStream } from '@/replication/stream'
import { ensureSetup, inspectSetup, teardown, type SetupStatus } from '@/setup'

export interface WalcastOptions {
  /** node-postgres connection string or config. */
  connection: string | pg.ClientConfig
  /** Publication name. Default: 'walcast'. */
  publication?: string
  /** Replication slot name. Default: 'walcast'. */
  slot?: string
  /** Restrict the publication to these tables when setup() creates it. */
  tables?: string[]
  /** Standby status update interval in ms. Default: 10_000. */
  statusIntervalMs?: number
  /** Raw frames buffered before backpressure pauses the socket. Default: 10_000. */
  highWaterMark?: number
}

/**
 * Logical replication client for Postgres. Library mode is the zero-plugin
 * experience: your own code is the sink.
 *
 * ```ts
 * const tr = new Walcast({ connection: process.env.DATABASE_URL! })
 * await tr.setup()
 * for await (const event of tr.changes()) {
 *   await handle(event)
 *   tr.ack(event)
 * }
 * ```
 *
 * Delivery is at-least-once: the replication slot's flushed position only
 * advances to what you `ack()`, so anything unacked at a crash is
 * redelivered on restart — with identical deterministic event ids.
 */
export class Walcast {
  readonly publication: string
  readonly slot: string

  private stream: ReplicationStream | null = null
  private iterating = false
  /**
   * Yielded-but-unacked events, insertion (= LSN) order. The value is what
   * acking the event advances the slot to: the event's own LSN, upgraded to
   * the transaction's commit end LSN once we know it was the last change.
   */
  private outstanding = new Map<string, { ackLsn: Lsn }>()

  constructor(private opts: WalcastOptions) {
    this.publication = opts.publication ?? 'walcast'
    this.slot = opts.slot ?? 'walcast'
  }

  private setupOpts() {
    const base = {
      connection: this.opts.connection,
      publication: this.publication,
      slot: this.slot,
    }
    return this.opts.tables ? { ...base, tables: this.opts.tables } : base
  }

  /** Create publication and slot if missing. Idempotent, never drops. */
  async setup(): Promise<void> {
    await ensureSetup(this.setupOpts())
  }

  /** Publication/slot/WAL status, including retained-WAL slot lag. */
  async status(): Promise<SetupStatus> {
    return inspectSetup(this.setupOpts())
  }

  /** Drop slot and publication. Destructive; see `walcast teardown`. */
  async teardown(): Promise<void> {
    await teardown(this.setupOpts())
  }

  /**
   * Start replication and yield decoded change events in commit order.
   *
   * Frames are decoded eagerly as they arrive (so commit records advance the
   * slot even while the consumer is between pulls); decoded events buffer up
   * to `highWaterMark`, beyond which the replication socket is paused — we
   * backpressure Postgres rather than drop.
   *
   * A single Walcast instance supports one active iteration (a replication
   * slot allows one consumer). Ends when `stop()` is called or the signal
   * aborts; throws on connection failure.
   */
  async *changes(opts: { signal?: AbortSignal } = {}): AsyncGenerator<ChangeEvent> {
    if (this.iterating) throw new Error('changes() is already being consumed on this instance')
    this.iterating = true
    this.outstanding.clear()

    // Set by Begin; consulted while pumping a transaction's changes.
    let commitLsn: Lsn = 0n
    let commitTime = ''
    let changeIndex = 0
    let lastEventId: string | null = null
    let inTransaction = false

    let stream: ReplicationStream
    const events = new AsyncQueue<ChangeEvent>({
      highWaterMark: this.opts.highWaterMark ?? 10_000,
      onPause: () => stream.pause(),
      onResume: () => stream.resume(),
    })

    try {
      stream = await ReplicationStream.start({
        connection: this.opts.connection,
        slot: this.slot,
        publication: this.publication,
        ...(this.opts.statusIntervalMs !== undefined
          ? { statusIntervalMs: this.opts.statusIntervalMs }
          : {}),
      })
    } catch (err) {
      this.iterating = false
      throw err
    }
    this.stream = stream
    const mine = stream
    const onAbort = () => void mine.stop()
    opts.signal?.addEventListener('abort', onAbort, { once: true })

    const decoder = new PgoutputDecoder()
    const emit = (event: ChangeEvent, walStart: Lsn) => {
      changeIndex++
      lastEventId = event.id
      this.outstanding.set(event.id, { ackLsn: walStart })
      events.push(event)
    }

    // Pump: decode raw frames into events the moment they arrive.
    const pump = (async () => {
      for await (const frame of mine.messages) {
        if (frame.tag === 'PrimaryKeepalive') {
          // Frames arrive in wire order, so at this point everything before
          // the keepalive is decoded. If it is also all acked and we are not
          // mid-transaction, the keepalive position is safe to confirm — an
          // idle consumer must not make the slot retain WAL produced by
          // unrelated tables. (A transaction still committing later than
          // walEnd is unaffected: redelivery keys off the commit position.)
          if (this.outstanding.size === 0 && !inTransaction && events.size === 0) {
            mine.updateFlushed(frame.walEnd)
          }
          continue
        }
        const msg = decoder.decode(frame.payload)
        switch (msg.tag) {
          case 'begin':
            commitLsn = msg.commitLsn
            commitTime = msg.commitTime.toISOString()
            changeIndex = 0
            lastEventId = null
            inTransaction = true
            break

          case 'commit': {
            // Acking the transaction's last event now releases the whole
            // transaction, commit record included.
            const last = lastEventId ? this.outstanding.get(lastEventId) : undefined
            if (last) last.ackLsn = msg.endLsn
            // Everything already acked (or an empty transaction): advance
            // the slot past the commit record immediately.
            if (this.outstanding.size === 0) mine.updateFlushed(msg.endLsn)
            lastEventId = null
            inTransaction = false
            break
          }

          case 'insert':
          case 'update':
          case 'delete':
            emit(
              {
                id: `${formatLsn(commitLsn)}:${changeIndex}`,
                lsn: formatLsn(frame.walStart),
                commit_lsn: formatLsn(commitLsn),
                commit_time: commitTime,
                schema: msg.relation.schema,
                table: msg.relation.name,
                op: msg.tag,
                before: msg.tag === 'insert' ? null : msg.old,
                after: msg.tag === 'delete' ? null : msg.new,
              },
              frame.walStart,
            )
            break

          case 'truncate':
            for (const rel of msg.relations) {
              emit(
                {
                  id: `${formatLsn(commitLsn)}:${changeIndex}`,
                  lsn: formatLsn(frame.walStart),
                  commit_lsn: formatLsn(commitLsn),
                  commit_time: commitTime,
                  schema: rel.schema,
                  table: rel.name,
                  op: 'truncate',
                  before: null,
                  after: null,
                },
                frame.walStart,
              )
            }
            break

          case 'relation':
          case 'type':
          case 'origin':
            break // bookkeeping messages; the decoder caches relations
        }
      }
    })().then(
      () => events.end(),
      (err: unknown) => events.fail(err),
    )

    try {
      yield* events
    } finally {
      opts.signal?.removeEventListener('abort', onAbort)
      await mine.stop()
      await pump
      // Another iteration may already have replaced us after stop().
      if (this.stream === mine || this.stream === null) {
        this.stream = null
        this.iterating = false
      }
    }
  }

  /**
   * Acknowledge an event (or a raw LSN string) as durably processed.
   * Cumulative in **delivery order**, like a Kafka offset commit: acking an
   * event also acknowledges everything delivered before it. The slot's
   * flushed position never advances past the newest ack, so unacked work is
   * redelivered after a crash.
   *
   * Delivery order — not LSN order — is the correct sweep axis: with
   * interleaved transactions, a change of a later-committing transaction can
   * sit at a *lower* WAL position than an earlier commit's end. Sweeping by
   * LSN comparison would drop such an entry as a side effect of acking the
   * earlier commit and then let the slot advance past work nobody processed.
   * Advancing flushed past a still-outstanding lower change LSN is safe:
   * pgoutput redelivers by commit position, and that commit is later.
   */
  ack(eventOrLsn: ChangeEvent | string): void {
    if (typeof eventOrLsn === 'string') {
      // Raw-LSN acks can't be placed in delivery order; they only move the
      // flushed position and clear the unambiguous outstanding prefix.
      const target = parseLsn(eventOrLsn)
      for (const [id, entry] of this.outstanding) {
        if (entry.ackLsn > target) break
        this.outstanding.delete(id)
      }
      this.stream?.updateFlushed(target)
      return
    }

    const entry = this.outstanding.get(eventOrLsn.id)
    const target = entry ? entry.ackLsn : parseLsn(eventOrLsn.lsn)
    if (entry) {
      // Maps iterate in insertion order == delivery order; remove the
      // prefix up to and including the acked event.
      for (const id of this.outstanding.keys()) {
        this.outstanding.delete(id)
        if (id === eventOrLsn.id) break
      }
    }
    this.stream?.updateFlushed(target)
  }

  /** Events yielded but not yet acked. */
  get pending(): number {
    return this.outstanding.size
  }

  /** The flushed LSN currently reported to Postgres. */
  get flushedLsn(): string | null {
    return this.stream ? formatLsn(this.stream.flushedLsn) : null
  }

  /** Stop replication; the active changes() iteration ends. */
  async stop(): Promise<void> {
    await this.stream?.stop()
    // The generator may be suspended at a yield and never resumed; release
    // the instance for a fresh changes() call regardless.
    this.stream = null
    this.iterating = false
  }
}
