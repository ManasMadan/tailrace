import pg from 'pg'
import { LSN_ZERO, formatLsn, type Lsn } from '@/lsn'
import { AsyncQueue } from '@/queue'
import {
  buildStandbyStatusUpdate,
  parseReplicationMessage,
  type ReplicationMessage,
} from '@/replication/protocol'

/**
 * pg's low-level connection object. Not part of pg's public typings, but it
 * is the documented escape hatch for copy-both mode: we receive walsender
 * CopyData frames via 'copyData' and push standby feedback with
 * sendCopyFromChunk. Everything inside those frames is parsed/built by us.
 */
interface PgLowLevelConnection {
  on(event: 'copyData', cb: (msg: { chunk: Buffer }) => void): void
  once(event: 'replicationStart', cb: () => void): void
  sendCopyFromChunk(chunk: Buffer): void
  stream: { pause(): void; resume(): void }
}

export interface ReplicationStreamOptions {
  /** node-postgres connection config; `replication: 'database'` is added. */
  connection: string | pg.ClientConfig
  slot: string
  publication: string
  /**
   * Where to start. Defaults to 0/0, which tells the server to resume from
   * the slot's confirmed_flush_lsn — the normal restart-safe behavior.
   */
  startLsn?: Lsn
  /** How often to send unsolicited standby status updates. Default 10s. */
  statusIntervalMs?: number
  /** Buffered frames before the socket is paused. Default 10_000. */
  highWaterMark?: number
}

/**
 * A live START_REPLICATION session: an async iterable of replication frames
 * (XLogData and keepalives, in wire order) plus the standby feedback loop.
 * Keepalives flow through the same queue as data so a consumer deciding
 * "everything up to this keepalive is processed" sees them strictly *after*
 * any data frames that arrived first — an out-of-band callback here could
 * confirm WAL whose transactions are still sitting undecoded in the queue.
 *
 * The flushed LSN reported to Postgres only moves when the consumer calls
 * updateFlushed() — the server is free to recycle WAL below it, so it must
 * reflect acknowledged work and nothing more optimistic.
 */
export class ReplicationStream {
  readonly messages: AsyncQueue<ReplicationMessage>

  private client: pg.Client
  private conn!: PgLowLevelConnection
  private flushed: Lsn
  private lastReceived: Lsn = LSN_ZERO
  private serverWalEndLsn: Lsn = LSN_ZERO
  private statusTimer?: NodeJS.Timeout
  private stopped = false

  private constructor(private opts: ReplicationStreamOptions) {
    this.flushed = opts.startLsn ?? LSN_ZERO
    this.messages = new AsyncQueue<ReplicationMessage>({
      highWaterMark: opts.highWaterMark ?? 10_000,
      onPause: () => this.conn.stream.pause(),
      onResume: () => this.conn.stream.resume(),
    })
    const config =
      typeof opts.connection === 'string' ? { connectionString: opts.connection } : opts.connection
    this.client = new pg.Client({ ...config, replication: 'database' } as pg.ClientConfig)
  }

  static async start(opts: ReplicationStreamOptions): Promise<ReplicationStream> {
    const stream = new ReplicationStream(opts)
    await stream.begin()
    return stream
  }

  private async begin(): Promise<void> {
    await this.client.connect()
    this.conn = (this.client as unknown as { connection: PgLowLevelConnection }).connection

    this.conn.on('copyData', ({ chunk }) => this.onCopyData(chunk))
    this.client.on('error', (err) => this.messages.fail(err))
    this.client.on('end', () => this.messages.end())

    const start = formatLsn(this.opts.startLsn ?? LSN_ZERO)
    const sql =
      `START_REPLICATION SLOT "${this.opts.slot}" LOGICAL ${start} ` +
      `(proto_version '1', publication_names '"${this.opts.publication}"')`

    const started = new Promise<void>((resolve) => this.conn.once('replicationStart', resolve))
    // The query promise stays pending for the lifetime of the stream; it
    // rejects if the server refuses (missing slot, slot in use, bad option).
    const refused = new Promise<never>((_, reject) => {
      this.client.query(sql).catch((err: Error) => {
        if (!this.stopped) reject(err)
      })
    })
    await Promise.race([started, refused])

    const interval = this.opts.statusIntervalMs ?? 10_000
    this.statusTimer = setInterval(() => this.sendStatus(), interval)
    this.statusTimer.unref()
    // Failures after startup (network drop, server shutdown) surface through
    // the consumer's iteration, so `refused` must not become an unhandled
    // rejection once we are streaming.
    refused.catch((err) => this.messages.fail(err))
  }

  private onCopyData(chunk: Buffer): void {
    let msg
    try {
      msg = parseReplicationMessage(chunk)
    } catch (err) {
      this.messages.fail(err)
      return
    }
    this.serverWalEndLsn = msg.walEnd
    if (msg.tag === 'PrimaryKeepalive') {
      if (msg.walEnd > this.lastReceived) this.lastReceived = msg.walEnd
      // Reply promptly (feedback is time-sensitive), but leave any flush
      // advancement to the consumer, which sees this frame in order.
      if (msg.replyRequested) this.sendStatus()
    } else if (msg.walStart > this.lastReceived) {
      this.lastReceived = msg.walStart
    }
    this.messages.push(msg)
  }

  /**
   * Advance the flushed position reported to Postgres. Monotonic: passing an
   * older LSN is a no-op. Call this only for work that is fully processed.
   */
  updateFlushed(lsn: Lsn): void {
    if (lsn > this.flushed) this.flushed = lsn
  }

  get flushedLsn(): Lsn {
    return this.flushed
  }

  /** Highest WAL position received from the server so far. */
  get receivedLsn(): Lsn {
    return this.lastReceived
  }

  /** Server's current end-of-WAL, from the latest frame. */
  get serverWalEnd(): Lsn {
    return this.serverWalEndLsn
  }

  /** Pause reading from the socket (downstream backpressure). */
  pause(): void {
    this.conn.stream.pause()
  }

  /** Resume reading from the socket. */
  resume(): void {
    this.conn.stream.resume()
  }

  /** Send a Standby status update now (also runs on a timer). */
  sendStatus(replyRequested = false): void {
    if (this.stopped) return
    try {
      this.conn.sendCopyFromChunk(
        buildStandbyStatusUpdate(this.lastReceived, this.flushed, this.flushed, replyRequested),
      )
    } catch (err) {
      this.messages.fail(err)
    }
  }

  async stop(): Promise<void> {
    if (this.stopped) return
    this.sendStatus() // report the final flushed position before disconnecting
    this.stopped = true
    if (this.statusTimer) clearInterval(this.statusTimer)
    this.messages.end()
    await this.client.end().catch(() => {})
  }
}
