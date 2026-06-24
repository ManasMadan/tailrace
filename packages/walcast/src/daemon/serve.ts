import { randomBytes } from 'node:crypto'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import type { Sink, SinkFactory } from '@walcast/plugin-kit'
import { NO_SINKS_ERROR, loadConfig, type DaemonConfig } from '@/daemon/config'
import { DaemonServer } from '@/daemon/server'
import { SinkEngine, type EngineSinkSpec } from '@/engine/engine'
import { createLogger } from '@/logger'
import { Walcast } from '@/walcast'

async function loadSink(entry: DaemonConfig['sinks'][number]): Promise<Sink> {
  // Plugins are installed in the *user's* project, not inside walcast —
  // resolve them from the working directory (also handles ./local-sink.js).
  let specifier = entry.use
  try {
    const requireFromCwd = createRequire(join(process.cwd(), 'package.json'))
    specifier = pathToFileURL(requireFromCwd.resolve(entry.use)).href
  } catch {
    // fall back to bare-specifier import below (e.g. node_modules of walcast itself)
  }
  let mod: { default?: SinkFactory }
  try {
    mod = (await import(specifier)) as { default?: SinkFactory }
  } catch (err) {
    throw new Error(
      `could not load sink '${entry.use}' — is it installed? (npm install ${entry.use})\n  ${
        err instanceof Error ? err.message : String(err)
      }`,
    )
  }
  if (typeof mod.default !== 'function') {
    throw new Error(
      `'${entry.use}' is not a walcast sink: its default export must be a factory (config) => Sink`,
    )
  }
  return mod.default(entry.config ?? {})
}

/**
 * `npx walcast serve` — load sink plugins, run the engine, expose the
 * control plane (admin API + dashboard). Returns a handle for tests.
 */
export async function serve(configPath?: string): Promise<{
  port: number
  stop: () => Promise<void>
  engine: SinkEngine
}> {
  const config = await loadConfig(configPath)
  const logger = createLogger(
    (process.env.WALCAST_LOG_LEVEL as 'debug' | 'info' | undefined) ?? 'info',
  )

  if (config.sinks.length === 0) {
    // The error message is onboarding: it must teach, not just fail. It also
    // outranks every other config problem — sinks are the concept to learn.
    console.error(NO_SINKS_ERROR)
    process.exitCode = 1
    throw new Error('no sinks configured')
  }
  if (!config.db) {
    throw new Error(
      'no database configured — set "db" in walcast.config.json, or DATABASE_URL / WALCAST_DB in the environment',
    )
  }

  const sinks: EngineSinkSpec[] = []
  const ids = new Set<string>()
  for (const entry of config.sinks) {
    const id = entry.name ?? entry.use.replace(/^@walcast\//, '')
    if (ids.has(id)) throw new Error(`duplicate sink name '${id}' — give each sink a unique "name"`)
    ids.add(id)
    sinks.push({ id, sink: await loadSink(entry), config: entry.config ?? {} })
  }

  const walcast = new Walcast({
    connection: config.db,
    publication: config.publication,
    slot: config.slot,
  })
  await walcast.setup()

  const authToken = config.server.authToken ?? randomBytes(24).toString('base64url')

  const engine = new SinkEngine({
    walcast,
    connection: config.db,
    sinks,
    logger,
    registerRoute: (sinkId, path, handler) => server.registerRoute(sinkId, path, handler),
    ...config.engine,
  })

  const uiDir = join(dirname(fileURLToPath(import.meta.url)), 'ui')
  const server = new DaemonServer({ engine, walcast, authToken, logger, uiDir })

  await engine.start()
  const port = await server.listen(config.server.port, config.server.host)

  logger.info('walcast daemon started', {
    port,
    sinks: sinks.map((s) => `${s.id} (${s.sink.durability})`),
    publication: config.publication,
    slot: config.slot,
  })
  if (!config.server.authToken) {
    logger.info('generated admin token (set server.authToken or WALCAST_AUTH_TOKEN to pin one)', {
      token: authToken,
    })
  }
  logger.info(`dashboard: http://${config.server.host}:${port}/ui/?token=${authToken}`)

  let stopping = false
  const stop = async () => {
    if (stopping) return
    stopping = true
    logger.info('shutting down')
    await engine.stop()
    await server.close()
  }
  process.once('SIGTERM', () => void stop().then(() => process.exit(0)))
  process.once('SIGINT', () => void stop().then(() => process.exit(0)))

  return { port, stop, engine }
}
