import { parseLsn, type Stats } from '@/api'

/**
 * The signature element: WAL as a river gauge. The rail spans from the
 * slot's restart_lsn (upstream — WAL still retained on disk) to the server's
 * confirmed flush / current position; each durable sink is a marker at its
 * acked LSN. Lag is literally distance.
 */
export function FlowRail({ stats }: { stats: Stats }) {
  const restart = parseLsn(stats.slot?.restartLsn ?? null)
  const confirmed = parseLsn(stats.slot?.confirmedFlushLsn ?? null)
  const flushed = parseLsn(stats.engine.flushedLsn)
  const head =
    confirmed !== null && flushed !== null
      ? flushed > confirmed
        ? flushed
        : confirmed
      : (confirmed ?? flushed)

  if (restart === null || head === null) {
    return (
      <div className="rounded-lg border border-line bg-basin p-6 text-sm text-muted">
        No slot data yet — waiting for the first poll of the replication stream.
      </div>
    )
  }

  const span = head - restart
  const pos = (lsn: bigint | null): number => {
    if (lsn === null || span <= 0n) return 100
    const clamped = lsn < restart ? restart : lsn > head ? head : lsn
    return Number(((clamped - restart) * 1000n) / span) / 10
  }

  const durables = stats.engine.sinks.filter((s) => s.durability === 'durable')

  return (
    <div className="rounded-lg border border-line bg-basin p-6">
      <div className="mb-6 flex items-baseline justify-between">
        <span className="eyebrow">wal flow</span>
        <span className="font-mono text-xs text-muted">
          retained ↔ {stats.slot?.restartLsn ?? '—'} …{' '}
          {stats.engine.flushedLsn ?? stats.slot?.confirmedFlushLsn ?? '—'}
        </span>
      </div>

      <div className="relative mx-2 h-16">
        {/* the race */}
        <div className="absolute top-7 h-1 w-full rounded bg-line" />
        <div
          className="absolute top-7 h-1 rounded bg-flow transition-all duration-700"
          style={{ width: `${pos(confirmed)}%` }}
        />
        {/* upstream + head labels */}
        <RailTick at={0} label="restart_lsn" mono={stats.slot?.restartLsn} side="below" />
        <RailTick
          at={100}
          label="head"
          mono={stats.engine.flushedLsn ?? stats.slot?.confirmedFlushLsn}
          side="below"
          align="right"
        />
        {/* sink markers */}
        {durables.map((s, i) => (
          <div
            key={s.id}
            className="absolute -translate-x-1/2 transition-all duration-700"
            style={{ left: `${pos(parseLsn(s.ackedLsn))}%`, top: i % 2 === 0 ? 0 : 40 }}
          >
            <div
              className={`mx-auto h-3 w-3 rounded-full border-2 border-ink ${
                s.status === 'paused' ? 'bg-warn' : 'bg-flow'
              }`}
              style={{ marginTop: i % 2 === 0 ? 16 : -14 }}
            />
            <div
              className={`font-mono text-[10px] ${s.status === 'paused' ? 'text-warn' : 'text-muted'}`}
            >
              {s.id}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function RailTick({
  at,
  label,
  mono,
  align,
}: {
  at: number
  label: string
  mono?: string | null | undefined
  side?: 'below'
  align?: 'right'
}) {
  return (
    <div
      className={`absolute top-10 ${align === 'right' ? '-translate-x-full text-right' : ''}`}
      style={{ left: `${at}%` }}
    >
      <div className="eyebrow">{label}</div>
      <div className="font-mono text-xs text-fg">{mono ?? '—'}</div>
    </div>
  )
}
