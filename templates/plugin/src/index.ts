import { appendFile } from 'node:fs/promises'
import type { ChangeEvent, Sink, SinkContext, SinkFactory } from '@walcast/plugin-kit'

/**
 * walcast-sink-example — a durable sink that appends one JSON line per
 * change event (NDJSON) to a file.
 *
 * It is intentionally tiny, but it honors every obligation of the Sink
 * contract, each one called out in a comment below. Replace the file-append
 * with your transport and you have a real sink.
 */

export interface ExampleSinkConfig {
  /** File that receives one JSON line per event. */
  path: string
}

class ExampleSink implements Sink {
  // `name` shows up in daemon logs and the dashboard.
  readonly name = 'example'

  // CONTRACT — durability. This sink is 'durable': the engine will not
  // advance our checkpoint (and, transitively, the replication slot) until
  // `deliver` resolves, and a rejected `deliver` is retried with backoff
  // until it succeeds or the sink is paused — never skipped.
  //
  // Declare 'ephemeral' instead if delivery is best-effort (a live feed,
  // a metrics tap): failures are then logged rather than retried, and the
  // sink can never hold WAL back. Declaring 'durable' when you don't mean
  // it stalls the pipeline; declaring 'ephemeral' when you need durability
  // loses events. This sink writes to disk, so it earns 'durable'.
  readonly durability = 'durable' as const

  private cfg: ExampleSinkConfig
  private ctx!: SinkContext

  // CONTRACT — construction is synchronous and side-effect-free. Validate
  // config here (throwing gives the user an error at startup, before any
  // replication begins), but do all I/O in `init`.
  constructor(config: Record<string, unknown>) {
    if (typeof config.path !== 'string' || config.path.length === 0) {
      throw new Error('walcast-sink-example: config.path must be a file path')
    }
    this.cfg = { path: config.path }
  }

  // CONTRACT — init(ctx) is called exactly once, before any delivery.
  // `ctx.config` is the same object the factory received; `ctx.resumeLsn`
  // tells a durable sink the last LSN it acknowledged (null on first run).
  // Everything at or below resumeLsn has been delivered before — though a
  // crash-recovery redelivery may still replay the tail, so don't treat it
  // as an exactly-once promise.
  async init(ctx: SinkContext): Promise<void> {
    this.ctx = ctx
    ctx.logger.info('example sink ready', {
      path: this.cfg.path,
      resumeLsn: ctx.resumeLsn,
    })
  }

  // CONTRACT — deliver(batch) receives batches in strict commit order.
  // Resolve = delivered, the engine may advance our checkpoint.
  // Throw = failed, the engine retries the *same* batch with backoff.
  // Never swallow a transport error and resolve anyway — for a durable
  // sink that silently drops events.
  //
  // CONTRACT — tolerate redelivery. walcast is at-least-once: after a
  // crash or a partially-applied failure, the same batch arrives again
  // with identical event ids (`event.id` is derived from the commit LSN,
  // stable across redelivery). This sink just appends, so a redelivered
  // batch produces duplicate lines — and that's fine, because consumers of
  // the file deduplicate on `event.id`. If your target can't tolerate
  // duplicates, make the write idempotent (upsert keyed on `event.id`,
  // transactional checkpoint, ...) instead.
  async deliver(batch: ChangeEvent[]): Promise<void> {
    const lines = batch.map((event) => JSON.stringify(event) + '\n').join('')
    // appendFile with the default flag 'a' is atomic enough per call for
    // NDJSON: each deliver appends its whole payload in one operation.
    await appendFile(this.cfg.path, lines, 'utf8')
    this.ctx.logger.debug('appended batch', {
      events: batch.length,
      lastId: batch[batch.length - 1]?.id,
    })
  }

  // CONTRACT — close() flushes and releases resources; the engine calls it
  // on shutdown, and it must be safe to call more than once. appendFile
  // opens and closes the file per call, so there is nothing to release here
  // — a sink holding a connection would end it here.
  async close(): Promise<void> {
    this.ctx?.logger.info('example sink closed')
  }
}

// CONTRACT — the package's default export is a factory (config) => Sink.
// The walcast daemon resolves this package from the user's node_modules,
// imports it, and calls the default export with the `config` object from
// the user's walcast configuration.
const factory: SinkFactory = (config) => new ExampleSink(config)
export default factory
