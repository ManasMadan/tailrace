import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtemp, writeFile } from 'node:fs/promises'
import { createServer, type Server } from 'node:http'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { afterAll, beforeAll, describe, expect, inject, it } from 'vitest'
import pg from 'pg'
import type { ChangeEvent } from '@walcast/plugin-kit'

const dsn = inject('dsn')

const CLI = join(dirname(createRequire(import.meta.url).resolve('walcast')), 'cli.js')
const DAEMON_PORT = 7719

function spawnDaemon(configPath: string): ChildProcess {
  return spawn(process.execPath, [CLI, 'serve', '--config', configPath], {
    env: { ...process.env, DATABASE_URL: dsn, WALCAST_AUTH_TOKEN: 'test-token' },
    cwd: join(dirname(new URL(import.meta.url).pathname), '..'), // resolve plugins like a user project
    stdio: ['ignore', 'pipe', 'pipe'],
  })
}

async function waitForHealthz(timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      const res = await fetch(`http://127.0.0.1:${DAEMON_PORT}/healthz`)
      if (res.ok) return
    } catch {
      /* not up yet */
    }
    if (Date.now() > deadline) throw new Error('daemon did not become healthy')
    await new Promise((r) => setTimeout(r, 150))
  }
}

describe.skipIf(!dsn)('daemon', () => {
  it('refuses to start with zero sinks, with a friendly onboarding error', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'walcast-nosink-'))
    const config = join(dir, 'walcast.config.json')
    await writeFile(config, JSON.stringify({ sinks: [] }))

    const child = spawnDaemon(config)
    let output = ''
    child.stdout?.on('data', (c: Buffer) => (output += c.toString()))
    child.stderr?.on('data', (c: Buffer) => (output += c.toString()))
    const code = await new Promise<number | null>((r) => child.on('exit', r))

    expect(code).toBe(1)
    expect(output).toContain('needs at least one sink')
    expect(output).toContain('npm install @walcast/sink-webhook')
    expect(output).toContain('@walcast/sink-kafka')
    expect(output).toContain('your code is the sink')
  })
})

describe.skipIf(!dsn)('money test #1: kill -9 under load, zero loss to webhooks', () => {
  let db: pg.Client
  let receiver: Server
  /** row n → times seen. Dupes allowed; gaps are not. */
  const seen = new Map<number, number>()

  beforeAll(async () => {
    db = new pg.Client({ connectionString: dsn })
    await db.connect()
    receiver = createServer((req, res) => {
      let body = ''
      req.on('data', (c: Buffer) => (body += c.toString()))
      req.on('end', () => {
        for (const e of JSON.parse(body) as ChangeEvent[]) {
          const n = (e.after as { n?: number } | null)?.n
          if (typeof n === 'number') seen.set(n, (seen.get(n) ?? 0) + 1)
        }
        res.writeHead(200).end()
      })
    })
    await new Promise<void>((r) => receiver.listen(0, '127.0.0.1', r))
  })

  afterAll(async () => {
    await db.end()
    await new Promise<void>((r) => receiver.close(() => r()))
  })

  it('every committed row reaches the webhook at least once across a kill -9', async () => {
    const addr = receiver.address()
    const hookUrl = `http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}/hook`

    await db.query(`DROP TABLE IF EXISTS money1`)
    await db.query(`CREATE TABLE money1 (id serial PRIMARY KEY, n int NOT NULL)`)
    await db.query(`SELECT pg_drop_replication_slot('slot_money1')`).catch(() => {})
    await db.query(`DROP PUBLICATION IF EXISTS pub_money1`).catch(() => {})
    await db.query(`DROP SCHEMA IF EXISTS walcast CASCADE`).catch(() => {})

    const dir = await mkdtemp(join(tmpdir(), 'walcast-money1-'))
    const config = join(dir, 'walcast.config.json')
    await writeFile(
      config,
      JSON.stringify({
        publication: 'pub_money1',
        slot: 'slot_money1',
        server: { port: DAEMON_PORT },
        engine: { lingerMs: 5 },
        sinks: [{ use: '@walcast/sink-webhook', name: 'hook', config: { url: hookUrl } }],
      }),
    )

    // Daemon #1 first — the slot must exist before the writer starts, or
    // early rows predate capture entirely (a slot only sees the future).
    const first = spawnDaemon(config)
    await waitForHealthz()

    // Continuous writer: 400 rows, one committed transaction each.
    const TOTAL = 400
    let inserted = 0
    const writer = (async () => {
      for (let n = 0; n < TOTAL; n++) {
        await db.query(`INSERT INTO money1 (n) VALUES ($1)`, [n])
        inserted++
        await new Promise((r) => setTimeout(r, 10))
      }
    })()
    const deadline = Date.now() + 30_000
    while (seen.size < 50 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50))
    expect(seen.size).toBeGreaterThanOrEqual(50)
    first.kill('SIGKILL')

    // Let the writer keep committing while nothing consumes the slot.
    await new Promise((r) => setTimeout(r, 1_000))

    // Daemon #2: resumes from the slot; everything unacked is redelivered.
    const second = spawnDaemon(config)
    await waitForHealthz()
    await writer
    expect(inserted).toBe(TOTAL)

    const doneBy = Date.now() + 60_000
    while (seen.size < TOTAL && Date.now() < doneBy) await new Promise((r) => setTimeout(r, 100))

    const missing = [...Array(TOTAL).keys()].filter((n) => !seen.has(n))
    expect(missing).toEqual([]) // at-least-once: nothing may be lost
    const duplicates = [...seen.values()].filter((count) => count > 1).length
    // Dupes are legal (that's what at-least-once means) — just report them.
    console.log(`money test #1: ${TOTAL} rows, ${duplicates} redelivered at least twice`)

    second.kill('SIGTERM')
    await new Promise((r) => second.on('exit', r))
  })
})
