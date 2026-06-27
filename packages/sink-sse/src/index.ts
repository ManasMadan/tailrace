import type { ServerResponse } from 'node:http'
import type { ChangeEvent, Sink, SinkContext, SinkFactory } from '@walcast/plugin-kit'

export interface SseSinkConfig {
  /** Heartbeat comment interval. Default 15_000 ms. */
  heartbeatMs?: number
}

interface Client {
  res: ServerResponse
  /** Filter from `?tables=a,b` (`schema.table` or bare table name). */
  tables: Set<string> | null
}

/**
 * Ephemeral live tail: `GET /plugins/<sinkId>/events` streams change events
 * as Server-Sent Events for dashboards and debugging.
 *
 * Semantics — the deliberate opposite of a durable sink: no client, no
 * delivery; a slow or dead client misses events; nothing here ever holds
 * the replication slot back. If you need events reliably, use a durable
 * sink; if you need to *watch*, this is the one.
 */
class SseSink implements Sink {
  readonly name = 'sse'
  readonly durability = 'ephemeral' as const
  private clients = new Set<Client>()
  private heartbeat?: NodeJS.Timeout
  private ctx!: SinkContext

  constructor(private cfg: SseSinkConfig) {}

  async init(ctx: SinkContext): Promise<void> {
    this.ctx = ctx
    ctx.http.registerRoute('/events', (req, res) => {
      const url = new URL(req.url ?? '/', 'http://local')
      const tablesParam = url.searchParams.get('tables')
      const client: Client = {
        res,
        tables: tablesParam
          ? new Set(
              tablesParam
                .split(',')
                .map((t) => t.trim())
                .filter(Boolean),
            )
          : null,
      }
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      })
      res.write(`: walcast live tail — events are best-effort, see docs on durability\n\n`)
      this.clients.add(client)
      ctx.logger.info('sse client connected', { clients: this.clients.size })
      req.on('close', () => {
        this.clients.delete(client)
        ctx.logger.info('sse client disconnected', { clients: this.clients.size })
      })
    })

    this.heartbeat = setInterval(() => {
      for (const client of this.clients) client.res.write(`: heartbeat\n\n`)
    }, this.cfg.heartbeatMs ?? 15_000)
    this.heartbeat.unref()
  }

  async deliver(batch: ChangeEvent[]): Promise<void> {
    if (this.clients.size === 0) return // ephemeral: no listener, no delivery
    for (const event of batch) {
      const frame = `id: ${event.id}\nevent: change\ndata: ${JSON.stringify(event)}\n\n`
      for (const client of this.clients) {
        if (
          client.tables &&
          !client.tables.has(event.table) &&
          !client.tables.has(`${event.schema}.${event.table}`)
        ) {
          continue
        }
        // A dead socket must never fail the batch — best-effort by contract.
        if (!client.res.writableEnded) client.res.write(frame)
      }
    }
  }

  async close(): Promise<void> {
    if (this.heartbeat) clearInterval(this.heartbeat)
    for (const client of this.clients) {
      client.res.write(`: server shutting down\n\n`)
      client.res.end()
    }
    this.clients.clear()
    this.ctx?.logger.info('sse sink closed')
  }
}

const factory: SinkFactory = (config) => new SseSink(config)
export default factory
