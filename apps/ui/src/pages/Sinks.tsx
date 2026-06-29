import { useState } from 'react'
import { post, type Stats } from '@/api'

export function Sinks({ stats, refresh }: { stats: Stats | null; refresh: () => Promise<void> }) {
  const [busy, setBusy] = useState<string | null>(null)
  if (!stats) return <p className="text-sm text-muted">Connecting…</p>

  const act = async (id: string, verb: 'pause' | 'resume') => {
    setBusy(id)
    try {
      await post(`/api/sinks/${encodeURIComponent(id)}/${verb}`)
      await refresh()
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="mx-auto max-w-4xl">
      <h1 className="eyebrow mb-6">sinks</h1>
      {stats.engine.sinks.length === 0 && (
        <p className="text-sm text-muted">No sinks configured.</p>
      )}
      <div className="space-y-3">
        {stats.engine.sinks.map((s) => (
          <div key={s.id} className="rounded-lg border border-line bg-basin p-5">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span
                  className={`inline-block h-2.5 w-2.5 rounded-full ${
                    s.status === 'paused' ? 'bg-warn' : 'bg-flow'
                  }`}
                />
                <span className="font-mono text-base font-semibold">{s.id}</span>
                <span className="rounded border border-line px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wider text-muted">
                  {s.name} · {s.durability}
                </span>
              </div>
              {s.durability === 'durable' && (
                <button
                  disabled={busy === s.id}
                  onClick={() => void act(s.id, s.status === 'paused' ? 'resume' : 'pause')}
                  className={`rounded px-3 py-1.5 font-mono text-xs font-semibold transition-colors focus-visible:outline focus-visible:outline-flow disabled:opacity-50 ${
                    s.status === 'paused'
                      ? 'bg-flow text-ink hover:brightness-110'
                      : 'border border-line text-muted hover:text-fg'
                  }`}
                >
                  {s.status === 'paused' ? 'Resume delivery' : 'Pause'}
                </button>
              )}
            </div>

            <dl className="mt-4 grid grid-cols-4 gap-4 font-mono text-xs">
              <Cell label="acked lsn" value={s.ackedLsn ?? '—'} />
              <Cell label="queue depth" value={String(s.queueDepth)} />
              <Cell label="delivered" value={s.deliveredCount.toLocaleString()} />
              <Cell
                label={s.durability === 'ephemeral' ? 'dropped (best-effort)' : 'status'}
                value={s.durability === 'ephemeral' ? String(s.droppedCount) : s.status}
              />
            </dl>

            {s.status === 'paused' && s.lastError && (
              <div className="mt-3 rounded border border-warn/40 bg-warn/10 px-3 py-2 text-xs">
                <span className="mr-2 font-mono font-semibold text-warn">paused with error</span>
                <span className="text-fg">{s.lastError}</span>
                <p className="mt-1 text-muted">
                  Delivery stopped after repeated failures. Nothing is lost — the batch retries when
                  you resume, and the slot holds WAL until then.
                </p>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

function Cell({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="eyebrow mb-1">{label}</dt>
      <dd>{value}</dd>
    </div>
  )
}
