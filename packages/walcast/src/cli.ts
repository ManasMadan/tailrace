#!/usr/bin/env node
import { readFileSync, realpathSync } from 'node:fs'
import { createInterface } from 'node:readline/promises'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'
import { Walcast } from '@/walcast'

const HELP = `walcast — Postgres change data capture for Node

Usage:
  walcast setup      Create publication and replication slot (idempotent)
  walcast serve      Run the sink daemon (needs at least one sink plugin)
  walcast status     Show publication, slot, and retained-WAL lag
  walcast teardown   Drop slot and publication (asks for confirmation)
  walcast --version  Print version

Options:
  --db <url>            Postgres connection string (or DATABASE_URL)
  --config <path>       Daemon config file (default: walcast.config.json)
  --publication <name>  Publication name (default: walcast)
  --slot <name>         Slot name (default: walcast)
  --tables <a,b>        Limit publication to these tables (setup only)
  --yes                 Skip the teardown confirmation

An orphaned replication slot retains WAL forever and will fill the disk.
If you stop using walcast, run 'walcast teardown'. Monitor slot lag with:
  SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
  FROM pg_replication_slots;
`

function version(): string {
  const url = new URL('../package.json', import.meta.url)
  return (JSON.parse(readFileSync(url, 'utf8')) as { version: string }).version
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      db: { type: 'string' },
      config: { type: 'string' },
      publication: { type: 'string' },
      slot: { type: 'string' },
      tables: { type: 'string' },
      yes: { type: 'boolean', default: false },
      version: { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
  })

  if (values.version) {
    console.log(version())
    return 0
  }
  const command = positionals[0]
  if (values.help || !command) {
    console.log(HELP)
    return command ? 0 : 1
  }

  if (command === 'serve') {
    if (values.db) process.env.WALCAST_DB = values.db
    const { serve } = await import('@/daemon/serve')
    await serve(values.config)
    return new Promise<number>(() => {}) // runs until SIGINT/SIGTERM
  }

  const connection = values.db ?? process.env.DATABASE_URL
  if (!connection) {
    console.error('error: no database. Pass --db <url> or set DATABASE_URL.')
    return 1
  }

  const tr = new Walcast({
    connection,
    ...(values.publication ? { publication: values.publication } : {}),
    ...(values.slot ? { slot: values.slot } : {}),
    ...(values.tables ? { tables: values.tables.split(',').map((t) => t.trim()) } : {}),
  })

  switch (command) {
    case 'setup': {
      await tr.setup()
      const s = await tr.status()
      console.log(
        `publication '${tr.publication}': ready${s.publication.allTables ? ' (all tables)' : ''}`,
      )
      console.log(`slot '${tr.slot}': ready (confirmed_flush ${s.slot.confirmedFlushLsn})`)
      return 0
    }
    case 'status': {
      const s = await tr.status()
      console.log(JSON.stringify(s, null, 2))
      return 0
    }
    case 'teardown': {
      if (!values.yes) {
        const rl = createInterface({ input: process.stdin, output: process.stdout })
        const answer = await rl.question(
          `Drop slot '${tr.slot}' and publication '${tr.publication}'? ` +
            `Undelivered changes are lost permanently. [y/N] `,
        )
        rl.close()
        if (answer.trim().toLowerCase() !== 'y') {
          console.log('aborted')
          return 1
        }
      }
      await tr.teardown()
      console.log('slot and publication dropped')
      return 0
    }
    default:
      console.error(`error: unknown command '${command}'\n`)
      console.log(HELP)
      return 1
  }
}

// Invoked as a bin, not imported. npm bin shims are symlinks, so compare
// real paths — a naive argv[1] check silently no-ops under npx.
const invokedDirectly = (() => {
  try {
    return (
      Boolean(process.argv[1]) && realpathSync(process.argv[1]!) === fileURLToPath(import.meta.url)
    )
  } catch {
    return false
  }
})()
if (invokedDirectly) {
  main().then(
    (code) => process.exit(code),
    (err) => {
      console.error(`error: ${err instanceof Error ? err.message : err}`)
      process.exit(1)
    },
  )
}
