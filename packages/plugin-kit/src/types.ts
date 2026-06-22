import type { IncomingMessage, ServerResponse } from 'node:http'

/**
 * A decoded row change, as delivered to sinks. `id` is deterministic —
 * derived from the commit LSN and the change's index within its transaction
 * — so a redelivered event (walcast is at-least-once) carries the identical
 * id. That is what makes consumer-side idempotency possible.
 */
export interface ChangeEvent<Row extends Record<string, unknown> = Record<string, unknown>> {
  /** `${commit_lsn}:${index-within-transaction}`, e.g. `0/1A2B3C8:0`. */
  id: string
  /** WAL position of this individual change. */
  lsn: string
  /** Commit LSN of the containing transaction; events are ordered by it. */
  commit_lsn: string
  /** Transaction commit time (ISO 8601). */
  commit_time: string
  schema: string
  table: string
  op: 'insert' | 'update' | 'delete' | 'truncate'
  /** Previous row image; null unless REPLICA IDENTITY provides it. */
  before: Row | null
  /** New row image; null for delete/truncate. */
  after: Row | null
}

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void
  info(msg: string, fields?: Record<string, unknown>): void
  warn(msg: string, fields?: Record<string, unknown>): void
  error(msg: string, fields?: Record<string, unknown>): void
}

export type HttpHandler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>

/** Capabilities handed to a sink at init time. */
export interface SinkContext {
  /** The `config` object from the sink's entry in walcast's configuration. */
  config: Record<string, unknown>
  /** Structured logger, tagged with the sink's id. */
  logger: Logger
  /** Unique id of this sink instance (config `name`, defaults to package name). */
  sinkId: string
  /**
   * For durable sinks: the LSN this sink last acknowledged, or null on first
   * run. Deliveries resume after it; anything at or below has been delivered
   * (though a crash-recovery redelivery may still replay the tail — sinks
   * must tolerate redelivery regardless).
   */
  resumeLsn: string | null
  /**
   * Mount an inbound HTTP route on the daemon's server. `path` is relative
   * and namespaced under `/plugins/<sinkId>/`. This is how transport plugins
   * that need an endpoint (like SSE) get one without running a server.
   */
  http: {
    registerRoute(path: string, handler: HttpHandler): void
  }
}

/**
 * The walcast sink contract.
 *
 * The engine calls {@link Sink.deliver} with batches of events in strict
 * commit order.
 *
 * **Durable sinks** (`durability: 'durable'`):
 * - The engine never advances this sink's checkpoint (nor, transitively, the
 *   replication slot) until `deliver` resolves.
 * - A rejected `deliver` is retried with exponential backoff and jitter.
 *   After `max_attempts` the sink is **paused** — never skipped, never
 *   advanced — and can be resumed from the API/UI.
 * - The same batch MAY be delivered more than once (crash recovery, retry
 *   after a partially-applied failure). Event ids are stable across
 *   redelivery; sinks must be idempotent or tolerate duplicates.
 *
 * **Ephemeral sinks** (`durability: 'ephemeral'`):
 * - Delivery is best-effort: failures are logged, not retried.
 * - The sink is excluded from the slot's min-LSN computation and can never
 *   hold WAL back.
 */
export interface Sink {
  readonly name: string
  readonly durability: 'durable' | 'ephemeral'
  /** Called once before any delivery. Register routes, open connections. */
  init(ctx: SinkContext): Promise<void>
  /** Deliver one ordered batch. Throw to signal failure (see contract above). */
  deliver(batch: ChangeEvent[]): Promise<void>
  /** Flush and release resources. The engine calls this on shutdown. */
  close(): Promise<void>
}

/**
 * What a sink package's default export must be: a factory taking the sink's
 * `config` object. Keeping construction synchronous and side-effect-free
 * (do I/O in `init`) makes sinks trivially testable.
 */
export type SinkFactory = (config: Record<string, unknown>) => Sink
