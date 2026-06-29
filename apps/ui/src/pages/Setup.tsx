import { formatBytes, type Stats } from '@/api'

export function Setup({ stats }: { stats: Stats | null }) {
  if (!stats) return <p className="text-sm text-muted">Connecting…</p>

  return (
    <div className="mx-auto max-w-4xl">
      <h1 className="eyebrow mb-6">setup</h1>

      <div className="grid grid-cols-2 gap-4">
        <Panel title="publication">
          <Row k="exists" v={stats.publication?.exists ? 'yes' : 'no'} />
          <Row
            k="scope"
            v={
              stats.publication?.allTables === null
                ? '—'
                : stats.publication?.allTables
                  ? 'all tables'
                  : 'selected tables'
            }
          />
          <Row k="wal_level" v={stats.walLevel ?? '—'} />
        </Panel>

        <Panel title="replication slot">
          <Row k="exists" v={stats.slot?.exists ? 'yes' : 'no'} />
          <Row k="active" v={stats.slot?.active ? 'yes' : 'no'} />
          <Row k="restart_lsn" v={stats.slot?.restartLsn ?? '—'} />
          <Row k="confirmed_flush" v={stats.slot?.confirmedFlushLsn ?? '—'} />
          <Row k="retained wal" v={formatBytes(stats.slot?.retainedWalBytes ?? null)} />
        </Panel>
      </div>

      <div className="mt-4 rounded-lg border border-fail/30 bg-basin p-6">
        <div className="eyebrow mb-2 !text-fail">teardown</div>
        <p className="mb-3 max-w-2xl text-sm text-muted">
          Dropping the slot and publication permanently discards undelivered changes and releases
          retained WAL. Teardown is deliberately not a button — run it where you can read what it
          asks you:
        </p>
        <pre className="rounded bg-ink p-4 font-mono text-xs">npx walcast teardown</pre>
        <p className="mt-3 max-w-2xl text-xs text-muted">
          An abandoned replication slot retains WAL forever and will eventually fill the disk. If
          you stop using walcast, tear it down. Watch for lag with:{' '}
          <code className="text-fg">
            SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM
            pg_replication_slots;
          </code>
        </p>
      </div>
    </div>
  )
}

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-line bg-basin p-6">
      <div className="eyebrow mb-4">{title}</div>
      <dl>{children}</dl>
    </div>
  )
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex justify-between border-b border-line/50 py-1.5 font-mono text-xs last:border-b-0">
      <dt className="text-muted">{k}</dt>
      <dd>{v}</dd>
    </div>
  )
}
