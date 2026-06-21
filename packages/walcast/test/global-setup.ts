import { execFileSync } from 'node:child_process'
import type { TestProject } from 'vitest/node'
import pg from 'pg'

/**
 * Provides a logical-replication-enabled Postgres for integration tests.
 * Honors WALCAST_TEST_DSN if set (e.g. in CI with a service container);
 * otherwise starts a throwaway docker container. If neither is possible the
 * integration suite skips itself.
 */

const CONTAINER = 'walcast-test-pg'
const PORT = 54329
const DSN = `postgres://postgres:postgres@127.0.0.1:${PORT}/postgres`

declare module 'vitest' {
  interface ProvidedContext {
    dsn: string
  }
}

function docker(...args: string[]): string {
  return execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
}

async function waitForPostgres(dsn: string, timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const client = new pg.Client({ connectionString: dsn })
    try {
      await client.connect()
      await client.query('SELECT 1')
      return
    } catch (err) {
      if (Date.now() > deadline) throw err
      await new Promise((r) => setTimeout(r, 300))
    } finally {
      await client.end().catch(() => {})
    }
  }
}

let startedContainer = false

export async function setup(project: TestProject): Promise<void> {
  if (process.env.WALCAST_TEST_DSN) {
    project.provide('dsn', process.env.WALCAST_TEST_DSN)
    return
  }
  try {
    const running = docker('ps', '-q', '--filter', `name=^${CONTAINER}$`).trim()
    if (!running) {
      docker('rm', '-f', CONTAINER).toString() // clear any stopped leftover
      docker(
        'run',
        '-d',
        '--name',
        CONTAINER,
        '-e',
        'POSTGRES_PASSWORD=postgres',
        '-p',
        `${PORT}:5432`,
        'postgres:16-alpine',
        '-c',
        'wal_level=logical',
      )
      startedContainer = true
    }
    await waitForPostgres(DSN)
    project.provide('dsn', DSN)
  } catch {
    console.warn('[walcast tests] docker unavailable — integration tests will be skipped')
    project.provide('dsn', '')
  }
}

export function teardown(): void {
  if (startedContainer && !process.env.WALCAST_TEST_KEEP_PG) {
    try {
      docker('rm', '-f', CONTAINER)
    } catch {
      /* already gone */
    }
  }
}
