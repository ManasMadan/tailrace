import { useEffect, useRef, useState } from 'react'
import { getToken, type ChangeEvent, type Stats } from '@/api'

const OP_TONE: Record<ChangeEvent['op'], string> = {
  insert: 'text-flow',
  update: 'text-warn',
  delete: 'text-fail',
  truncate: 'text-fail',
}

export function Live({ stats }: { stats: Stats | null }) {
  const sse = stats?.engine.sinks.find((s) => s.name === 'sse')
  const [events, setEvents] = useState<ChangeEvent[]>([])
  const [tables, setTables] = useState('')
  const [connected, setConnected] = useState(false)
  const [expanded, setExpanded] = useState<string | null>(null)
  const streamRef = useRef<EventSource | null>(null)

  useEffect(() => {
    if (!sse) return
    const params = new URLSearchParams({ token: getToken() })
    if (tables.trim()) params.set('tables', tables.trim())
    const stream = new EventSource(`/plugins/${sse.id}/events?${params}`)
    streamRef.current = stream
    stream.onopen = () => setConnected(true)
    stream.onerror = () => setConnected(false)
    stream.addEventListener('change', (e) => {
      const event = JSON.parse((e as MessageEvent<string>).data) as ChangeEvent
      setEvents((prev) => [event, ...prev.slice(0, 199)])
    })
    return () => {
      stream.close()
      setConnected(false)
    }
  }, [sse?.id, tables])

  if (!stats) return <p className="text-sm text-muted">Connecting…</p>

  if (!sse) {
    return (
      <div className="mx-auto max-w-4xl">
        <h1 className="eyebrow mb-6">live inspector</h1>
        <div className="rounded-lg border border-line bg-basin p-8">
          <p className="mb-2 font-mono text-base font-semibold">
            The live tail needs the SSE sink.
          </p>
          <p className="mb-4 max-w-xl text-sm text-muted">
            The inspector streams events through <code className="text-fg">@walcast/sink-sse</code>{' '}
            — an ephemeral sink that can never hold the replication slot back. Install it and add it
            to your config:
          </p>
          <pre className="mb-3 rounded bg-ink p-4 font-mono text-xs leading-relaxed">
            {`npm install @walcast/sink-sse

// walcast.config.json
{ "sinks": [ …, { "use": "@walcast/sink-sse" } ] }`}
          </pre>
          <p className="text-xs text-muted">Restart the daemon and this page becomes the tail.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="mx-auto max-w-4xl">
      <div className="mb-6 flex items-center justify-between">
        <h1 className="eyebrow">live inspector</h1>
        <div className="flex items-center gap-3">
          <input
            value={tables}
            onChange={(e) => setTables(e.target.value)}
            placeholder="filter tables, e.g. users,orders"
            className="w-64 rounded border border-line bg-basin px-3 py-1.5 font-mono text-xs outline-none focus:border-flow"
          />
          <span className="flex items-center gap-1.5 font-mono text-xs text-muted">
            <span
              className={`inline-block h-2 w-2 rounded-full ${connected ? 'bg-flow' : 'bg-warn'}`}
            />
            {connected ? 'tailing' : 'reconnecting'}
          </span>
        </div>
      </div>

      {events.length === 0 ? (
        <div className="rounded-lg border border-line bg-basin p-8 text-sm text-muted">
          Waiting for changes — commit something to a published table and it appears here.
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-line">
          {events.map((e) => (
            <button
              key={e.id}
              onClick={() => setExpanded(expanded === e.id ? null : e.id)}
              className="surface block w-full border-b border-line bg-basin px-4 py-2 text-left last:border-b-0 hover:bg-basin-2 focus-visible:outline focus-visible:-outline-offset-2 focus-visible:outline-flow"
            >
              <div className="flex items-center gap-4 font-mono text-xs">
                <span className={`w-16 font-semibold uppercase ${OP_TONE[e.op]}`}>{e.op}</span>
                <span className="w-48 truncate text-fg">
                  {e.schema}.{e.table}
                </span>
                <span className="flex-1 truncate text-muted">
                  {e.after ? JSON.stringify(e.after) : e.before ? JSON.stringify(e.before) : '—'}
                </span>
                <span className="text-muted">{e.commit_lsn}</span>
              </div>
              {expanded === e.id && (
                <pre className="mt-2 overflow-x-auto rounded bg-ink p-3 text-left font-mono text-[11px] leading-relaxed text-fg">
                  {JSON.stringify(e, null, 2)}
                </pre>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
