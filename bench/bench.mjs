// End-to-end benchmark against real infrastructure:
//
//   pnpm -r build && npm run bench
//
// Starts (or reuses) throwaway Postgres 16 and Kafka containers, runs the
// daemon with webhook + Kafka(EOS) sinks, writes single-row transactions at
// full speed for a fixed window, and reports sustained throughput plus
// commit→webhook and commit→Kafka p50/p95 latency (event commit_time to
// arrival at the consumer, same host clock).
import { execFileSync, spawn } from 'node:child_process'
import { createServer } from 'node:http'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Kafka, logLevel } from 'kafkajs'
import pg from 'pg'

const WRITE_SECONDS = Number(process.env.BENCH_SECONDS ?? 15)
const WRITERS = Number(process.env.BENCH_WRITERS ?? 4)

const here = dirname(fileURLToPath(import.meta.url))
const CLI = join(here, '../packages/walcast/dist/cli.js')
const sh = (cmd, args) =>
  execFileSync(cmd, args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })

// --- infrastructure -------------------------------------------------------
function ensureContainer(name, runArgs) {
  if (sh('docker', ['ps', '-q', '--filter', `name=^${name}$`]).trim()) return
  try {
    sh('docker', ['rm', '-f', name])
  } catch {
    /* best effort */
  }
  sh('docker', ['run', '-d', '--name', name, ...runArgs])
}

const DSN =
  process.env.BENCH_DATABASE_URL ?? 'postgres://postgres:postgres@127.0.0.1:54329/postgres'
const BROKER = process.env.BENCH_KAFKA ?? '127.0.0.1:19092'

if (!process.env.BENCH_DATABASE_URL) {
  ensureContainer('walcast-test-pg', [
    '-e',
    'POSTGRES_PASSWORD=postgres',
    '-p',
    '54329:5432',
    'postgres:16-alpine',
    '-c',
    'wal_level=logical',
  ])
}
if (!process.env.BENCH_KAFKA) {
  ensureContainer('walcast-test-kafka', [
    '-p',
    '19092:9092',
    '-e',
    'KAFKA_NODE_ID=1',
    '-e',
    'KAFKA_PROCESS_ROLES=broker,controller',
    '-e',
    'KAFKA_LISTENERS=EXTERNAL://0.0.0.0:9092,INTERNAL://localhost:29092,CONTROLLER://localhost:9093',
    '-e',
    `KAFKA_ADVERTISED_LISTENERS=EXTERNAL://${BROKER},INTERNAL://localhost:29092`,
    '-e',
    'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
    '-e',
    'KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL',
    '-e',
    'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
    '-e',
    'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
    '-e',
    'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
    '-e',
    'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
    '-e',
    'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
    'apache/kafka:3.9.0',
  ])
}

async function waitFor(what, probe, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      await probe()
      return
    } catch (err) {
      if (Date.now() > deadline) throw new Error(`${what} never became ready: ${err}`)
      await new Promise((r) => setTimeout(r, 500))
    }
  }
}

await waitFor('postgres', async () => {
  const c = new pg.Client({ connectionString: DSN })
  await c.connect()
  await c.end()
})
const kafka = new Kafka({ clientId: 'bench', brokers: [BROKER], logLevel: logLevel.NOTHING })
await waitFor('kafka', async () => {
  const admin = kafka.admin()
  await admin.connect()
  await admin.listTopics()
  await admin.disconnect()
})

// --- schema + daemon ------------------------------------------------------
const db = new pg.Client({ connectionString: DSN })
await db.connect()
await db.query(`DROP TABLE IF EXISTS bench_orders`)
await db.query(
  `CREATE TABLE bench_orders (id serial PRIMARY KEY, n int NOT NULL, payload text NOT NULL)`,
)
await db.query(`SELECT pg_drop_replication_slot('slot_bench')`).catch(() => {})
await db.query(`DROP PUBLICATION IF EXISTS pub_bench`).catch(() => {})
await db.query(`DROP SCHEMA IF EXISTS walcast CASCADE`).catch(() => {})

/** n → { commit: ms epoch from the event, webhook: arrival, kafka: arrival } */
const samples = new Map()
const sample = (n) => {
  let s = samples.get(n)
  if (!s) samples.set(n, (s = {}))
  return s
}

const receiver = createServer((req, res) => {
  let body = ''
  req.on('data', (c) => (body += c))
  req.on('end', () => {
    const now = Date.now()
    for (const e of JSON.parse(body)) {
      const s = sample(e.after.n)
      s.commit = Date.parse(e.commit_time)
      if (s.webhook === undefined) s.webhook = now
    }
    res.writeHead(200).end()
  })
})
await new Promise((r) => receiver.listen(0, '127.0.0.1', r))
const hookPort = receiver.address().port

const prefix = `bench${Date.now()}`
const dir = mkdtempSync(join(tmpdir(), 'walcast-bench-'))
const configPath = join(dir, 'walcast.config.json')
writeFileSync(
  configPath,
  JSON.stringify({
    publication: 'pub_bench',
    slot: 'slot_bench',
    server: { port: 7727, authToken: 'bench' },
    engine: { batchSize: 500, lingerMs: 5 },
    sinks: [
      {
        use: '@walcast/sink-webhook',
        name: 'hook',
        config: { url: `http://127.0.0.1:${hookPort}/hook` },
      },
      {
        use: '@walcast/sink-kafka',
        name: 'kafka',
        config: { brokers: [BROKER], topicPrefix: prefix, transactionalId: `txn-${prefix}` },
      },
    ],
  }),
)

const daemon = spawn(process.execPath, [CLI, 'serve', '--config', configPath], {
  cwd: here,
  env: { ...process.env, DATABASE_URL: DSN, WALCAST_LOG_LEVEL: 'warn' },
  stdio: ['ignore', 'inherit', 'inherit'],
})
await waitFor('daemon', async () => {
  const res = await fetch('http://127.0.0.1:7727/healthz')
  if (!res.ok) throw new Error(String(res.status))
})

// Pre-create the data topic so the consumer can subscribe before traffic.
const admin = kafka.admin()
await admin.connect()
await admin.createTopics({ topics: [{ topic: `${prefix}.public.bench_orders`, numPartitions: 1 }] })
await admin.disconnect()

const consumer = kafka.consumer({ groupId: `bench-${prefix}`, readUncommitted: false })
await consumer.connect()
await consumer.subscribe({ topic: `${prefix}.public.bench_orders`, fromBeginning: true })
consumer
  .run({
    eachMessage: ({ message }) => {
      const now = Date.now()
      const e = JSON.parse(message.value.toString())
      const s = sample(e.after.n)
      s.commit = Date.parse(e.commit_time)
      if (s.kafka === undefined) s.kafka = now
      return Promise.resolve()
    },
  })
  .catch((err) => console.error('kafka consumer failed:', err))

// --- write phase ----------------------------------------------------------
console.log(`writing single-row transactions with ${WRITERS} writers for ${WRITE_SECONDS}s…`)
const payload = 'x'.repeat(120)
let n = 0
const stopAt = Date.now() + WRITE_SECONDS * 1000
const writers = Array.from({ length: WRITERS }, async () => {
  const client = new pg.Client({ connectionString: DSN })
  await client.connect()
  while (Date.now() < stopAt) {
    await client.query(`INSERT INTO bench_orders (n, payload) VALUES ($1, $2)`, [n++, payload])
  }
  await client.end()
})
const writeStart = Date.now()
await Promise.all(writers)
const writeSeconds = (Date.now() - writeStart) / 1000
const total = n
console.log(
  `wrote ${total} rows in ${writeSeconds.toFixed(1)}s (${Math.round(total / writeSeconds)} tx/s)`,
)

// --- drain + report -------------------------------------------------------
const drained = (key) => [...samples.values()].filter((s) => s[key] !== undefined).length
await waitFor(
  'webhook drain',
  () =>
    drained('webhook') >= total
      ? Promise.resolve()
      : Promise.reject(new Error(`${drained('webhook')}/${total}`)),
  120_000,
)
await waitFor(
  'kafka drain',
  () =>
    drained('kafka') >= total
      ? Promise.resolve()
      : Promise.reject(new Error(`${drained('kafka')}/${total}`)),
  120_000,
)

function percentiles(key) {
  const lat = [...samples.values()]
    .filter((s) => s[key] !== undefined && s.commit !== undefined)
    .map((s) => s[key] - s.commit)
    .sort((a, b) => a - b)
  const at = (p) => lat[Math.min(lat.length - 1, Math.floor((p / 100) * lat.length))]
  return { p50: at(50), p95: at(95), n: lat.length }
}

const webhook = percentiles('webhook')
const kafkaLat = percentiles('kafka')
console.log('\n== walcast bench ==')
console.log(
  `sustained throughput   ${Math.round(total / writeSeconds)} events/s (${total} single-row transactions, ${WRITERS} writers, ${writeSeconds.toFixed(1)}s)`,
)
console.log(`commit → webhook       p50 ${webhook.p50} ms, p95 ${webhook.p95} ms (n=${webhook.n})`)
console.log(
  `commit → kafka (EOS)   p50 ${kafkaLat.p50} ms, p95 ${kafkaLat.p95} ms (n=${kafkaLat.n}, read_committed)`,
)

await consumer.disconnect().catch(() => {})
daemon.kill('SIGTERM')
await new Promise((r) => daemon.on('exit', r))
receiver.close()
await db.end()
