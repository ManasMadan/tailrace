import { createHmac, timingSafeEqual } from 'node:crypto'
import type { ChangeEvent, Sink, SinkContext, SinkFactory } from '@walcast/plugin-kit'

export interface WebhookSinkConfig {
  /** Endpoint that receives `POST` batches (a JSON array of events). */
  url: string
  /** HMAC-SHA256 secret; the signature travels in X-Walcast-Signature. */
  secret?: string
  /** Extra headers, e.g. an Authorization header for the receiver. */
  headers?: Record<string, string>
  /** Per-request timeout. Default 30_000 ms. */
  timeoutMs?: number
}

/** `sha256=<hex hmac of the raw body>` — verify with a timing-safe compare. */
export function signBody(body: string, secret: string): string {
  return `sha256=${createHmac('sha256', secret).update(body).digest('hex')}`
}

/** For receivers: constant-time signature check. */
export function verifySignature(body: string, secret: string, signature: string): boolean {
  const expected = Buffer.from(signBody(body, secret))
  const got = Buffer.from(signature)
  return expected.length === got.length && timingSafeEqual(expected, got)
}

/**
 * Durable webhook delivery. The engine guarantees ordering and retries with
 * backoff (throwing here is the signal); this sink is deliberately nothing
 * but transport: serialize, sign, POST, insist on a 2xx.
 *
 * Delivery is at-least-once — receivers deduplicate on `event.id`, which is
 * identical across redeliveries.
 */
class WebhookSink implements Sink {
  readonly name = 'webhook'
  readonly durability = 'durable' as const
  private ctx!: SinkContext
  private cfg: WebhookSinkConfig

  constructor(config: Record<string, unknown>) {
    const url = config.url
    if (typeof url !== 'string' || !/^https?:\/\//.test(url)) {
      throw new Error(
        `@walcast/sink-webhook: config.url must be an http(s) URL, got ${JSON.stringify(url)}`,
      )
    }
    this.cfg = {
      url,
      ...(typeof config.secret === 'string' ? { secret: config.secret } : {}),
      ...(config.headers && typeof config.headers === 'object'
        ? { headers: config.headers as Record<string, string> }
        : {}),
      ...(typeof config.timeoutMs === 'number' ? { timeoutMs: config.timeoutMs } : {}),
    }
  }

  async init(ctx: SinkContext): Promise<void> {
    this.ctx = ctx
    ctx.logger.info('webhook sink ready', {
      url: this.cfg.url,
      signed: Boolean(this.cfg.secret),
      resumeLsn: ctx.resumeLsn,
    })
  }

  async deliver(batch: ChangeEvent[]): Promise<void> {
    const body = JSON.stringify(batch)
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'user-agent': 'walcast-webhook',
      'x-walcast-batch-size': String(batch.length),
      'x-walcast-first-id': batch[0]?.id ?? '',
      'x-walcast-last-id': batch[batch.length - 1]?.id ?? '',
      ...this.cfg.headers,
    }
    if (this.cfg.secret) headers['x-walcast-signature'] = signBody(body, this.cfg.secret)

    const res = await fetch(this.cfg.url, {
      method: 'POST',
      headers,
      body,
      signal: AbortSignal.timeout(this.cfg.timeoutMs ?? 30_000),
    })
    // Drain to keep the connection reusable, then judge the status.
    await res.arrayBuffer().catch(() => {})
    if (!res.ok) {
      throw new Error(`webhook receiver responded ${res.status} for ${batch.length} events`)
    }
  }

  async close(): Promise<void> {
    this.ctx?.logger.info('webhook sink closed')
  }
}

const factory: SinkFactory = (config) => new WebhookSink(config)
export default factory
