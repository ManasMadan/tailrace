import { describe, expect, it } from 'vitest'
import { formatLsn, parseLsn, pgTimeToDate } from '@/lsn'

describe('LSN', () => {
  it('round-trips text form', () => {
    for (const text of ['0/0', '0/1A2B3C4', '16/B374D848', 'FFFFFFFF/FFFFFFFF']) {
      expect(formatLsn(parseLsn(text))).toBe(text)
    }
  })

  it('parses into an orderable bigint', () => {
    expect(parseLsn('0/FFFFFFFF') < parseLsn('1/0')).toBe(true)
    expect(parseLsn('16/B374D848')).toBe((0x16n << 32n) | 0xb374d848n)
  })

  it('rejects malformed input', () => {
    for (const bad of ['', '16', '1/2/3', 'xyz/123', '123456789/0']) {
      expect(() => parseLsn(bad)).toThrow(/invalid LSN/)
    }
  })

  it('converts Postgres timestamps (µs since 2000-01-01)', () => {
    expect(pgTimeToDate(0n).toISOString()).toBe('2000-01-01T00:00:00.000Z')
  })
})
