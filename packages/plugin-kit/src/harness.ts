import assert from 'node:assert/strict'
import type { ChangeEvent, HttpHandler, Logger, SinkContext, SinkFactory } from '@/types'

export interface VerifySinkOptions {
  /** Sink config passed to the factory and exposed on the context. */
  config?: Record<string, unknown>
  /**
   * How the harness observes what actually crossed the transport: return
   * every event received at the far end, in arrival order. A webhook sink's
   * test passes a capturing HTTP server; a Kafka sink's test reads the topic
   * back. Without it the harness can only check the sink's local behavior.
   */
  collect?: () => Promise<ChangeEvent[]>
  /**
   * Sinks with exactly-once semantics into their transport (e.g. Kafka EOS)
   * must deduplicate redelivered batches; set true to assert no duplicates
   * arrive. Default false: redelivery produces duplicates, which is fine.
   */
  expectDedupe?: boolean
  /** Run before init / after close, for external fixtures. */
  before?: () => Promise<void>
  after?: () => Promise<void>
  /**
   * Runs right after init(), before any delivery — attach clients to routes
   * the sink registered (an SSE sink needs a listener before events flow).
   */
  afterInit?: (ctx: MockSinkContext) => Promise<void>
  /** Number of events in the generated workload. Default 20. */
  eventCount?: number
}

/** Deterministic synthetic workload with walcast-shaped ids and LSNs. */
export function makeTestEvents(count: number, startLsn = 1000): ChangeEvent[] {
  const events: ChangeEvent[] = []
  for (let i = 0; i < count; i++) {
    const commit = (startLsn + i * 8).toString(16).toUpperCase()
    events.push({
      id: `0/${commit}:0`,
      lsn: `0/${commit}`,
      commit_lsn: `0/${commit}`,
      commit_time: new Date(1_700_000_000_000 + i * 1000).toISOString(),
      schema: 'public',
      table: 'conformance',
      op: i % 4 === 3 ? 'delete' : i % 2 === 0 ? 'insert' : 'update',
      before: i % 2 === 0 ? null : { id: i, v: `old-${i}` },
      after: i % 4 === 3 ? null : { id: i, v: `new-${i}` },
    })
  }
  return events
}

class CollectingLogger implements Logger {
  entries: Array<{ level: string; msg: string; fields?: Record<string, unknown> }> = []
  private log(level: string, msg: string, fields?: Record<string, unknown>) {
    this.entries.push(fields ? { level, msg, fields } : { level, msg })
  }
  debug = (m: string, f?: Record<string, unknown>) => this.log('debug', m, f)
  info = (m: string, f?: Record<string, unknown>) => this.log('info', m, f)
  warn = (m: string, f?: Record<string, unknown>) => this.log('warn', m, f)
  error = (m: string, f?: Record<string, unknown>) => this.log('error', m, f)
}

export interface MockSinkContext extends SinkContext {
  logger: CollectingLogger
  routes: Map<string, HttpHandler>
}

/** A SinkContext double: captures logs and route registrations. */
export function makeMockContext(
  config: Record<string, unknown> = {},
  sinkId = 'conformance-test',
): MockSinkContext {
  const routes = new Map<string, HttpHandler>()
  return {
    config,
    sinkId,
    resumeLsn: null,
    logger: new CollectingLogger(),
    routes,
    http: {
      registerRoute(path: string, handler: HttpHandler) {
        assert.ok(path.startsWith('/'), `route path must start with '/': ${path}`)
        assert.ok(!routes.has(path), `route registered twice: ${path}`)
        routes.set(path, handler)
      },
    },
  }
}

/**
 * Conformance harness for the sink contract. Every official sink passes it
 * in CI — through the same door community plugins use. Throws (AssertionError)
 * on the first violation; resolves when the sink conforms.
 *
 * Checks:
 * 1. sane metadata (name, durability)
 * 2. init(ctx) resolves and may register namespaced routes
 * 3. batches are delivered in order, order preserved end-to-end (`collect`)
 * 4. redelivery of an identical batch is tolerated (and deduplicated when
 *    `expectDedupe` — the exactly-once-into-transport case)
 * 5. close() resolves and is idempotent
 *
 * What it deliberately can't check generically: failure behavior against a
 * broken transport (an ephemeral sink must log-and-continue, a durable sink
 * must throw so the engine retries). Inducing transport failure is
 * transport-specific — cover it in your sink's own tests, the way the
 * official sinks do.
 */
export async function verifySink(
  factory: SinkFactory,
  opts: VerifySinkOptions = {},
): Promise<void> {
  await opts.before?.()
  try {
    const sink = factory(opts.config ?? {})

    // 1. metadata
    assert.ok(typeof sink.name === 'string' && sink.name.length > 0, 'sink.name must be non-empty')
    assert.ok(
      sink.durability === 'durable' || sink.durability === 'ephemeral',
      `sink.durability must be 'durable' | 'ephemeral', got ${String(sink.durability)}`,
    )

    // 2. init
    const ctx = makeMockContext(opts.config ?? {}, `verify-${sink.name}`)
    await sink.init(ctx)
    await opts.afterInit?.(ctx)

    // 3. ordered delivery across several batches
    const events = makeTestEvents(opts.eventCount ?? 20)
    const batches = [events.slice(0, 7), events.slice(7, 8), events.slice(8)]
    for (const batch of batches) {
      const attempt = sink.deliver(batch)
      assert.ok(attempt instanceof Promise, 'deliver must return a Promise')
      await attempt
    }

    // 4. redelivery of the exact same batch — the crash-recovery case
    await sink.deliver(batches[1]!)

    if (opts.collect) {
      const seen = await opts.collect()
      const seenIds = seen.map((e) => e.id)
      const expectedIds = events.map((e) => e.id)
      if (opts.expectDedupe) {
        assert.deepEqual(
          seenIds,
          expectedIds,
          'transport must contain exactly one copy of each event, in order',
        )
      } else {
        // At-least-once: every event present, order preserved among firsts.
        const firstSeen = seenIds.filter((id, i) => seenIds.indexOf(id) === i)
        assert.deepEqual(
          firstSeen,
          expectedIds,
          'transport must receive every event, order preserved',
        )
      }
    }

    // 6. close, idempotently
    await sink.close()
    await sink.close()
  } finally {
    await opts.after?.()
  }
}
