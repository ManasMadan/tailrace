// Installability proof, run in CI: pack the real tarballs, install them into
// a scratch project the way a stranger would, and verify:
//   1. `import { Walcast } from 'walcast'` works
//   2. `npx walcast --version` works
//   3. the daemon refuses to start with zero sinks, with the friendly error
//   4. with @walcast/sink-webhook configured, the daemon actually serves
//      (needs docker for a throwaway Postgres; skipped without it)
import { execFileSync, spawn } from 'node:child_process'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const run = (cmd, args, opts = {}) =>
  execFileSync(cmd, args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], ...opts })

const scratch = mkdtempSync(join(tmpdir(), 'walcast-scratch-'))
console.log(`scratch project: ${scratch}`)

// Pack workspace packages (workspace:^ ranges are rewritten by pnpm pack).
const tarballs = {}
for (const dir of ['walcast', 'plugin-kit', 'sink-webhook']) {
  const out = run('pnpm', ['pack', '--out', join(scratch, `${dir}.tgz`)], {
    cwd: join(root, 'packages', dir),
  })
  tarballs[dir] = join(scratch, `${dir}.tgz`)
  console.log(`packed ${dir}: ${out.trim().split('\n').pop()}`)
}

writeFileSync(join(scratch, 'package.json'), JSON.stringify({ name: 'scratch', type: 'module' }))
run('npm', ['install', tarballs['plugin-kit'], tarballs['walcast'], tarballs['sink-webhook']], {
  cwd: scratch,
})

// 1. library import
const imported = run(
  process.execPath,
  ['-e', `import('walcast').then(m => console.log(typeof m.Walcast, typeof m.PgoutputDecoder))`],
  { cwd: scratch },
)
if (imported.trim() !== 'function function') throw new Error(`library import broken: ${imported}`)
console.log('1. import { Walcast } — ok')

// 2. CLI
const version = run('npx', ['walcast', '--version'], { cwd: scratch }).trim()
if (!/^\d+\.\d+\.\d+/.test(version)) throw new Error(`CLI version broken: ${version}`)
console.log(`2. npx walcast --version — ok (${version})`)

// 3. zero sinks → friendly onboarding error, exit code 1 (no DB needed)
writeFileSync(join(scratch, 'walcast.config.json'), JSON.stringify({ sinks: [] }))
let zeroSinkOutput = ''
try {
  run('npx', ['walcast', 'serve'], { cwd: scratch })
  throw new Error('daemon started with zero sinks — it must refuse')
} catch (err) {
  zeroSinkOutput = `${err.stdout ?? ''}${err.stderr ?? ''}`
}
if (
  !zeroSinkOutput.includes('needs at least one sink') ||
  !zeroSinkOutput.includes('@walcast/sink-webhook')
) {
  throw new Error(`zero-sink error is not the friendly one:\n${zeroSinkOutput}`)
}
console.log('3. zero-sink startup refused with the onboarding error — ok')

// 4. with a sink configured, the daemon serves (throwaway Postgres via docker)
let hasDocker = true
try {
  run('docker', ['info'])
} catch {
  hasDocker = false
  console.log('4. docker unavailable — skipping the live-daemon check')
}
if (hasDocker) {
  const container = 'walcast-scratch-pg'
  try {
    run('docker', ['rm', '-f', container])
  } catch {
    /* best effort */
  }
  run('docker', [
    'run',
    '-d',
    '--name',
    container,
    '-e',
    'POSTGRES_PASSWORD=postgres',
    '-p',
    '54333:5432',
    'postgres:16-alpine',
    '-c',
    'wal_level=logical',
  ])
  try {
    const dsn = 'postgres://postgres:postgres@127.0.0.1:54333/postgres'
    for (let i = 0; ; i++) {
      try {
        run('docker', ['exec', container, 'pg_isready', '-U', 'postgres'])
        break
      } catch {
        if (i > 60) throw new Error('postgres never became ready')
        await new Promise((r) => setTimeout(r, 500))
      }
    }
    writeFileSync(
      join(scratch, 'walcast.config.json'),
      JSON.stringify({
        server: { port: 7723, authToken: 'scratch' },
        sinks: [{ use: '@walcast/sink-webhook', config: { url: 'http://127.0.0.1:9/never' } }],
      }),
    )
    const daemon = spawn('npx', ['walcast', 'serve'], {
      cwd: scratch,
      env: { ...process.env, DATABASE_URL: dsn },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let ok = false
    for (let i = 0; i < 100 && !ok; i++) {
      try {
        const res = await fetch('http://127.0.0.1:7723/healthz')
        ok = res.ok
      } catch {
        /* best effort */
      }
      await new Promise((r) => setTimeout(r, 200))
    }
    daemon.kill('SIGTERM')
    if (!ok) throw new Error('daemon with webhook sink never became healthy')
    console.log('4. daemon serves with @walcast/sink-webhook from config — ok')
  } finally {
    try {
      run('docker', ['rm', '-f', container])
    } catch {
      /* best effort */
    }
  }
}

console.log('tarball installability: all checks passed')
