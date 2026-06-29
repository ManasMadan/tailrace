import type { RateSample } from '@/App'

/** Hand-rolled SVG stream: events/sec over the last ~2 minutes. */
export function Sparkline({ samples }: { samples: RateSample[] }) {
  const W = 560
  const H = 64
  if (samples.length < 2) {
    return (
      <div className="flex h-16 items-center font-mono text-xs text-muted">gathering samples…</div>
    )
  }
  const max = Math.max(1, ...samples.map((s) => s.rate))
  const step = W / Math.max(59, samples.length - 1)
  const points = samples.map(
    (s, i) => `${(i * step).toFixed(1)},${(H - 6 - (s.rate / max) * (H - 14)).toFixed(1)}`,
  )
  const area = `0,${H} ${points.join(' ')} ${((samples.length - 1) * step).toFixed(1)},${H}`

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-16 w-full" preserveAspectRatio="none" aria-hidden>
      <polygon points={area} fill="var(--color-flow)" opacity="0.08" />
      <polyline
        points={points.join(' ')}
        fill="none"
        stroke="var(--color-flow)"
        strokeWidth="1.5"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  )
}
