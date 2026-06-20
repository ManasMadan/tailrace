import { compareEventIds, type ChangeEvent } from '@walcast/plugin-kit'

/**
 * The event schema lives in @walcast/plugin-kit — it is the contract sinks
 * are written against, and plugin authors depend only on the kit. The core
 * re-exports it so library users never need a second import.
 */
export type { ChangeEvent }

/**
 * Total order of events: by commit LSN, then by index within the
 * transaction (both encoded in the deterministic id).
 */
export function compareEvents(a: Pick<ChangeEvent, 'id'>, b: Pick<ChangeEvent, 'id'>): number {
  return compareEventIds(a.id, b.id)
}
