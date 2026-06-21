import type { Logger } from '@walcast/plugin-kit'

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

const LEVELS: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 }

/**
 * Minimal JSON-lines logger. One line per entry on stdout, machine-parseable,
 * no dependencies. Sinks receive a child tagged with their id.
 */
export function createLogger(
  minLevel: LogLevel = 'info',
  base: Record<string, unknown> = {},
  write: (line: string) => void = (line) => process.stdout.write(line + '\n'),
): Logger & { child(fields: Record<string, unknown>): Logger } {
  const emit = (level: LogLevel, msg: string, fields?: Record<string, unknown>) => {
    if (LEVELS[level] < LEVELS[minLevel]) return
    write(JSON.stringify({ ts: new Date().toISOString(), level, msg, ...base, ...fields }))
  }
  return {
    debug: (msg, fields) => emit('debug', msg, fields),
    info: (msg, fields) => emit('info', msg, fields),
    warn: (msg, fields) => emit('warn', msg, fields),
    error: (msg, fields) => emit('error', msg, fields),
    child: (fields) => createLogger(minLevel, { ...base, ...fields }, write),
  }
}
