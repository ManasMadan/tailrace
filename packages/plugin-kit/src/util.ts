/**
 * Event ids are `commit_lsn:index` (`0/1A2B3C8:0`). Parsing and comparing
 * them is how a deduplicating sink decides "at or below my checkpoint" —
 * shared here so every sink does it the same way.
 */

/** Postgres text LSN (`X/Y`) → orderable bigint. */
export function lsnToBigint(text: string): bigint {
  const m = /^([0-9A-Fa-f]{1,8})\/([0-9A-Fa-f]{1,8})$/.exec(text)
  if (!m) throw new Error(`invalid LSN: ${JSON.stringify(text)}`)
  return (BigInt(`0x${m[1]}`) << 32n) | BigInt(`0x${m[2]}`)
}

/**
 * Total order over event ids: by commit LSN, then by index within the
 * transaction. Returns <0, 0, >0 like a comparator.
 */
export function compareEventIds(a: string, b: string): number {
  const [alsn = '0/0', aidx = '0'] = a.split(':')
  const [blsn = '0/0', bidx = '0'] = b.split(':')
  const byLsn = lsnToBigint(alsn) - lsnToBigint(blsn)
  if (byLsn !== 0n) return byLsn < 0n ? -1 : 1
  return Number(aidx) - Number(bidx)
}
