import type pg from 'pg'
import type { ChangeEvent, HttpHandler, Logger, Sink } from '@walcast/plugin-kit'
import { compareEvents } from '@/events'
import { BoundedQueue } from '@/engine/bounded-queue'
import { CheckpointStore } from '@/engine/checkpoints'
import { createLogger } from '@/logger'
import type { Walcast } from '@/walcast'

export interface EngineSinkSpec {
  /** Unique instance id (config `name`, defaults to the package name). */
  id: string
  sink: Sink
  config: Record<string, unknown>
}

export interface SinkEngineOptions {
  walcast: Walcast
  /** Connection for the checkpoint store (regular, non-replication). */
  connection: string | pg.ClientConfig
  sinks: EngineSinkSpec[]
  logger?: Logger & { child(fields: Record<string, unknown>): Logger }
  /** Provided by the daemon so sinks can mount HTTP routes. */
  registerRoute?: (sinkId: string, path: string, handler: HttpHandler) => void
  /** Max events per deliver() batch. Default 100. */
  batchSize?: number
  /** How long to wait for a fuller batch after the first event. Default 25ms. */
  lingerMs?: number
  /** Delivery attempts before a durable sink is paused. Default 10. */
  maxAttempts?: number
  backoffBaseMs?: number
  backoffCapMs?: number
  /** Per-sink buffered events before backpressure (durable) / drops (ephemeral). Default 1000. */
  queueDepth?: number
}

interface Tracked {
  seq: number
  event: ChangeEvent
  /** Counts toward ack ordering but is never handed to the sink. */
  skip?: boolean
}

interface SinkState {
  spec: EngineSinkSpec
  queue: BoundedQueue<Tracked>
  status: 'running' | 'paused'
  lastError: string | null
  /** Highest seq this sink has fully delivered (or skipped as pre-delivered). */
  deliveredSeq: number
  deliveredCount: number
  droppedCount: number
  ackedLsn: string | null
  ackedEventId: string | null
  resume: (() => void) | null
  worker?: Promise<void>
}

export interface SinkStats {
  id: string
  name: string
  durability: 'durable' | 'ephemeral'
  status: 'running' | 'paused'
  lastError: string | null
  queueDepth: number
  deliveredCount: number
  droppedCount: number
  ackedLsn: string | null
}

export interface EngineStats {
  eventsTotal: number
  flushedLsn: string | null
  sinks: SinkStats[]
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/**
 * The sink engine: owns ordering, batching, retries, backpressure, and
 * per-sink LSN checkpointing. Plugins own transport, nothing else.
 *
 * - Events fan out to every sink in commit order.
 * - Durable sinks: bounded queues; when full, the engine stops reading from
 *   the replication stream (WAL then accumulates on the server — never
 *   dropped). Failed deliveries retry with exponential backoff + jitter;
 *   after `maxAttempts` the sink pauses with its last error and holds the
 *   slot until resumed.
 * - Ephemeral sinks: best-effort. Failures are logged, a full queue drops
 *   events, and they never appear in the slot's min-LSN computation.
 * - The replication slot advances to the minimum acked LSN across durable
 *   sinks only.
 */
export class SinkEngine {
  readonly checkpoints: CheckpointStore
  private states: SinkState[] = []
  private log: Logger & { child(fields: Record<string, unknown>): Logger }
  private seq = 0
  private ackedSeq = 0
  private bySeq = new Map<number, ChangeEvent>()
  private abort = new AbortController()
  private pumpDone: Promise<void> | null = null
  private eventsTotal = 0

  constructor(private opts: SinkEngineOptions) {
    this.checkpoints = new CheckpointStore(opts.connection)
    this.log = opts.logger ?? createLogger()
  }

  async start(): Promise<void> {
    await this.checkpoints.ensure()

    for (const spec of this.opts.sinks) {
      const durable = spec.sink.durability === 'durable'
      const checkpoint = durable ? await this.checkpoints.register(spec.id) : null
      const state: SinkState = {
        spec,
        queue: new BoundedQueue<Tracked>(this.opts.queueDepth ?? 1_000),
        status: checkpoint?.status ?? 'running',
        lastError: checkpoint?.lastError ?? null,
        deliveredSeq: 0,
        deliveredCount: 0,
        droppedCount: 0,
        ackedLsn: checkpoint?.ackedLsn ?? null,
        ackedEventId: checkpoint?.ackedEventId ?? null,
        resume: null,
      }
      await spec.sink.init({
        config: spec.config,
        logger: this.log.child({ sink: spec.id }),
        sinkId: spec.id,
        resumeLsn: state.ackedLsn,
        http: {
          registerRoute: (path, handler) => {
            if (!this.opts.registerRoute) {
              throw new Error(
                `sink '${spec.id}' wants an HTTP route but the engine has no server (library mode?)`,
              )
            }
            this.opts.registerRoute(spec.id, path, handler)
          },
        },
      })
      this.states.push(state)
    }

    for (const state of this.states) {
      state.worker =
        state.spec.sink.durability === 'durable'
          ? this.durableWorker(state)
          : this.ephemeralWorker(state)
    }
    this.pumpDone = this.pump()
  }

  /**
   * Fan events out to sink queues; backpressure via durable queue puts.
   * Reconnects with backoff on replication failures — a killed predecessor
   * can hold the slot for a moment, and network blips must not be fatal.
   * At-least-once semantics make reconnect-and-redeliver always safe.
   */
  private async pump(): Promise<void> {
    for (let attempt = 0; !this.abort.signal.aborted;) {
      try {
        await this.pumpOnce()
        if (this.abort.signal.aborted) return // stopped on purpose
        // A clean end without stop() is a server-side disconnect.
        this.log.warn('replication stream ended; reconnecting')
        await sleep(500)
      } catch (err) {
        if (this.abort.signal.aborted) return
        attempt++
        const backoff = Math.min(10_000, 500 * 2 ** Math.min(attempt, 5))
        this.log.warn('replication stream failed; reconnecting', {
          error: err instanceof Error ? err.message : String(err),
          retryInMs: backoff,
        })
        await sleep(backoff)
      }
    }
  }

  private async pumpOnce(): Promise<void> {
    const tr = this.opts.walcast
    const durables = this.states.filter((s) => s.spec.sink.durability === 'durable')
    try {
      for await (const event of tr.changes({ signal: this.abort.signal })) {
        const seq = ++this.seq
        this.bySeq.set(seq, event)
        // Never fan out our own metadata writes: with FOR ALL TABLES the
        // checkpoint updates in walcast.sinks would otherwise generate
        // events, whose delivery writes checkpoints — a feedback loop. They
        // still traverse durable queues as skip markers so ack ordering
        // stays intact (acking around them would release queued user events).
        if (event.schema === 'walcast') {
          if (durables.length === 0) {
            tr.ack(event)
            this.bySeq.delete(seq)
          } else {
            for (const state of durables) await state.queue.put({ seq, event, skip: true })
          }
          continue
        }
        this.eventsTotal++
        const tracked: Tracked = { seq, event }
        for (const state of this.states) {
          if (state.spec.sink.durability === 'durable') {
            // After a restart the slot resumes from the minimum acked LSN;
            // sinks that were ahead skip what they already delivered.
            if (state.ackedEventId && compareEvents(event, { id: state.ackedEventId }) <= 0) {
              state.deliveredSeq = seq
              continue
            }
            await state.queue.put(tracked)
          } else if (state.queue.size < (this.opts.queueDepth ?? 1_000)) {
            void state.queue.put(tracked)
          } else {
            state.droppedCount++
            if (state.droppedCount % 1_000 === 1) {
              this.log.warn('ephemeral sink queue full, dropping events', {
                sink: state.spec.id,
                dropped: state.droppedCount,
              })
            }
          }
        }
        if (durables.length === 0) {
          // Nothing durable holds the slot; the library may advance freely.
          tr.ack(event)
          this.bySeq.delete(seq)
        } else {
          this.maybeAck()
        }
      }
    } finally {
      // Only shut the pipeline down for good on engine stop; a reconnect
      // must leave the sink workers running.
      if (this.abort.signal.aborted) {
        for (const state of this.states) state.queue.close()
      }
    }
  }

  /** Advance the slot to the min delivered position across durable sinks. */
  private maybeAck(): void {
    let min = Infinity
    for (const state of this.states) {
      if (state.spec.sink.durability !== 'durable') continue
      min = Math.min(min, state.deliveredSeq)
    }
    if (!Number.isFinite(min) || min <= this.ackedSeq) return
    const event = this.bySeq.get(min)
    if (event) this.opts.walcast.ack(event)
    for (let s = this.ackedSeq + 1; s <= min; s++) this.bySeq.delete(s)
    this.ackedSeq = min
  }

  private async durableWorker(state: SinkState): Promise<void> {
    const { sink } = state.spec
    const maxAttempts = this.opts.maxAttempts ?? 10
    const baseMs = this.opts.backoffBaseMs ?? 200
    const capMs = this.opts.backoffCapMs ?? 30_000

    // status flips concurrently (pause API, failure handling) — a plain
    // property read narrows and trips TS2367 on the second check.
    const paused = () => state.status === 'paused'
    for (;;) {
      // An operator pause (API/UI) takes effect at the next batch boundary;
      // in-flight deliveries finish rather than being cut mid-request.
      while (paused()) {
        if (this.abort.signal.aborted) return
        await this.waitForResume(state)
      }
      const batch = await state.queue.takeBatch(
        this.opts.batchSize ?? 100,
        this.opts.lingerMs ?? 25,
      )
      if (batch === null) return
      while (paused()) {
        // Paused while we waited for this batch: hold it and deliver on resume.
        if (this.abort.signal.aborted) return
        await this.waitForResume(state)
      }

      const deliverable = batch.filter((t) => !t.skip).map((t) => t.event)
      let attempt = 0
      for (; deliverable.length > 0;) {
        if (this.abort.signal.aborted) return
        try {
          await sink.deliver(deliverable)
          break
        } catch (err) {
          attempt++
          state.lastError = err instanceof Error ? err.message : String(err)
          if (attempt >= maxAttempts) {
            this.log.error('sink paused after repeated delivery failures', {
              sink: state.spec.id,
              attempts: attempt,
              error: state.lastError,
            })
            state.status = 'paused'
            await this.checkpoints.setStatus(state.spec.id, 'paused', state.lastError)
            await this.waitForResume(state)
            if (this.abort.signal.aborted) return
            attempt = 0 // resumed: retry the same batch from scratch
          } else {
            const backoff = Math.min(capMs, baseMs * 2 ** (attempt - 1))
            await sleep(backoff / 2 + Math.random() * (backoff / 2)) // full jitter, ≥ half
          }
        }
      }

      const last = batch[batch.length - 1]!
      state.deliveredSeq = last.seq
      state.deliveredCount += deliverable.length
      state.ackedLsn = last.event.commit_lsn
      state.ackedEventId = last.event.id
      state.lastError = null
      // Skip-only batches (our own walcast.sinks metadata events) advance
      // in-memory state and the slot, but must not write a checkpoint row:
      // that UPDATE would itself generate a skip event — a write loop that
      // never quiesces on an otherwise idle database.
      if (deliverable.length > 0) {
        await this.checkpoints.ack(state.spec.id, last.event.commit_lsn, last.event.id)
      }
      this.maybeAck()
    }
  }

  private async ephemeralWorker(state: SinkState): Promise<void> {
    const { sink } = state.spec
    for (;;) {
      const batch = await state.queue.takeBatch(
        this.opts.batchSize ?? 100,
        this.opts.lingerMs ?? 25,
      )
      if (batch === null) return
      try {
        await sink.deliver(batch.map((t) => t.event))
        state.deliveredCount += batch.length
      } catch (err) {
        // Best-effort by contract: log and move on, never retry, never block.
        state.lastError = err instanceof Error ? err.message : String(err)
        this.log.warn('ephemeral sink delivery failed (dropped)', {
          sink: state.spec.id,
          events: batch.length,
          error: state.lastError,
        })
      }
    }
  }

  private waitForResume(state: SinkState): Promise<void> {
    // Guard the already-aborted case: an abort listener added after abort()
    // never fires, and stop()'s resume sweep has already run — waiting here
    // would deadlock shutdown.
    if (this.abort.signal.aborted) return Promise.resolve()
    return new Promise((resolve) => {
      const onAbort = () => resolve()
      state.resume = () => {
        this.abort.signal.removeEventListener('abort', onAbort)
        resolve()
      }
      this.abort.signal.addEventListener('abort', onAbort, { once: true })
    })
  }

  /** Pause a durable sink manually (API/UI). */
  async pause(sinkId: string): Promise<void> {
    const state = this.mustFind(sinkId)
    state.status = 'paused'
    await this.checkpoints.setStatus(sinkId, 'paused', state.lastError ?? undefined)
  }

  /** Resume a paused sink; it retries its pending batch immediately. */
  async resume(sinkId: string): Promise<void> {
    const state = this.mustFind(sinkId)
    state.status = 'running'
    await this.checkpoints.setStatus(sinkId, 'running')
    state.resume?.()
    state.resume = null
  }

  private mustFind(sinkId: string): SinkState {
    const state = this.states.find((s) => s.spec.id === sinkId)
    if (!state) throw new Error(`unknown sink: ${sinkId}`)
    return state
  }

  stats(): EngineStats {
    return {
      eventsTotal: this.eventsTotal,
      flushedLsn: this.opts.walcast.flushedLsn,
      sinks: this.states.map((s) => ({
        id: s.spec.id,
        name: s.spec.sink.name,
        durability: s.spec.sink.durability,
        status: s.status,
        lastError: s.lastError,
        queueDepth: s.queue.size,
        deliveredCount: s.deliveredCount,
        droppedCount: s.droppedCount,
        ackedLsn: s.ackedLsn,
      })),
    }
  }

  async stop(): Promise<void> {
    this.abort.abort()
    await this.opts.walcast.stop()
    for (const state of this.states) {
      state.queue.close()
      state.resume?.()
    }
    await Promise.allSettled([this.pumpDone, ...this.states.map((s) => s.worker)])
    for (const state of this.states) {
      await state.spec.sink.close().catch((err: unknown) => {
        this.log.warn('sink close failed', { sink: state.spec.id, error: String(err) })
      })
    }
    await this.checkpoints.close()
  }
}
