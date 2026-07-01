import { spawn, type ChildProcess } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtemp, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { afterAll, beforeAll, describe, expect, inject, it } from 'vitest'
import { Kafka, logLevel } from 'kafkajs'
import pg from 'pg'
import type { ChangeEvent } from '@walcast/plugin-kit'

const dsn = inject('dsn')
const brokers = inject('brokers')

const CLI = join(dirname(createRequire(import.meta.url).resolve('walcast')), 'cli.js')
const DAEMON_PORT = 7721

function spawnDaemon(configPath: string): ChildProcess {
  return spawn(process.execPath, [CLI, 'serve', '--config', configPath], {
    env: { ...process.env, DATABASE_URL: dsn, WALCAST_AUTH_TOKEN: 'test-token' },
    cwd: join(dirname(new URL(import.meta.url).pathname), '..'),
    stdio: ['ignore', 'inherit', 'inherit'],
  })
}

async function waitForHealthz(timeoutMs = 30_000): Promise<void> {
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

/** Everything a read_committed consumer sees on the topic, in offset order. */
async function readCommitted(kafka: Kafka, topic: string, idleMs = 3_000): Promise<ChangeEvent[]> {
  const consumer = kafka.consumer({ groupId: `money2-${randomUUID()}`, readUncommitted: false })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  const out: ChangeEvent[] = []
  await new Promise<void>((resolve, reject) => {
    let idle = setTimeout(resolve, idleMs)
    consumer
      .run({
        eachMessage: ({ message }) => {
          clearTimeout(idle)
          idle = setTimeout(resolve, idleMs)
          if (message.value) out.push(JSON.parse(message.value.toString()) as ChangeEvent)
          return Promise.resolve()
        },
      })
      .catch(reject)
  })
  await consumer.disconnect()
  return out
}

describe.skipIf(!dsn || !brokers.length)(
  'money test #2: kill -9 during Kafka transactions → exactly one copy of everything',
  () => {
    let db: pg.Client
    const kafka = brokers.length
      ? new Kafka({ clientId: 'money2', brokers, logLevel: logLevel.NOTHING })
      : null!

    beforeAll(async () => {
      db = new pg.Client({ connectionString: dsn })
      await db.connect()
    })

    afterAll(async () => {
      await db.end()
    })

    it('read_committed consumers see every committed row exactly once', async () => {
      await db.query(`DROP TABLE IF EXISTS money2`)
      await db.query(`CREATE TABLE money2 (id serial PRIMARY KEY, n int NOT NULL)`)
      await db.query(`SELECT pg_drop_replication_slot('slot_money2')`).catch(() => {})
      await db.query(`DROP PUBLICATION IF EXISTS pub_money2`).catch(() => {})
      await db.query(`DROP SCHEMA IF EXISTS walcast CASCADE`).catch(() => {})

      const prefix = `money2${randomUUID().slice(0, 6)}`
      const dir = await mkdtemp(join(tmpdir(), 'walcast-money2-'))
      const config = join(dir, 'walcast.config.json')
      await writeFile(
        config,
        JSON.stringify({
          publication: 'pub_money2',
          slot: 'slot_money2',
          server: { port: DAEMON_PORT },
          engine: { lingerMs: 5, batchSize: 20 },
          sinks: [
            {
              use: '@walcast/sink-kafka',
              name: 'kafka',
              config: {
                brokers,
                topicPrefix: prefix,
                transactionalId: `txn-${prefix}`,
                transactionTimeoutMs: 10_000,
              },
            },
          ],
        }),
      )

      const first = spawnDaemon(config)
      await waitForHealthz()

      const TOTAL = 300
      const writer = (async () => {
        for (let n = 0; n < TOTAL; n++) {
          await db.query(`INSERT INTO money2 (n) VALUES ($1)`, [n])
          await new Promise((r) => setTimeout(r, 8))
        }
      })()

      // Kill without ceremony while transactions are in flight. The fixed
      // transactional.id fences the zombie; its open transaction is aborted
      // and stays invisible to read_committed consumers.
      await new Promise((r) => setTimeout(r, 900))
      first.kill('SIGKILL')
      await new Promise((r) => setTimeout(r, 500))

      const second = spawnDaemon(config)
      await waitForHealthz()
      await writer

      // Wait until the topic (read_committed) contains every row, then a
      // little longer to catch any straggling duplicates.
      const topic = `${prefix}.public.money2`
      const deadline = Date.now() + 90_000
      let seen: ChangeEvent[] = []
      for (;;) {
        seen = await readCommitted(kafka, topic)
        const distinct = new Set(seen.map((e) => (e.after as { n: number }).n))
        if (distinct.size >= TOTAL || Date.now() > deadline) break
        await new Promise((r) => setTimeout(r, 1_000))
      }

      const ns = seen.map((e) => (e.after as { n: number }).n).sort((a, b) => a - b)
      expect(ns.length).toBe(TOTAL) // exactly one copy of each — no dupes, no gaps
      expect(ns).toEqual([...Array(TOTAL).keys()])
      // Event ids are unique too (the stronger EOS statement).
      expect(new Set(seen.map((e) => e.id)).size).toBe(TOTAL)

      second.kill('SIGTERM')
      await new Promise((r) => second.on('exit', r))
    }, 180_000)
  },
)
