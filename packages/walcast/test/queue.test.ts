import { describe, expect, it } from 'vitest'
import { AsyncQueue } from '@/queue'

async function collect<T>(q: AsyncQueue<T>): Promise<T[]> {
  const out: T[] = []
  for await (const item of q) out.push(item)
  return out
}

describe('AsyncQueue', () => {
  it('delivers pushed items in order and ends', async () => {
    const q = new AsyncQueue<number>()
    q.push(1)
    q.push(2)
    const done = collect(q)
    q.push(3)
    q.end()
    expect(await done).toEqual([1, 2, 3])
  })

  it('wakes a waiting consumer', async () => {
    const q = new AsyncQueue<string>()
    const done = collect(q)
    q.push('a')
    q.end()
    expect(await done).toEqual(['a'])
  })

  it('propagates failure to the consumer', async () => {
    const q = new AsyncQueue<number>()
    const done = collect(q)
    q.fail(new Error('boom'))
    await expect(done).rejects.toThrow('boom')
  })

  it('fires onPause at the high-water mark and onResume after draining', async () => {
    const events: string[] = []
    const q = new AsyncQueue<number>({
      highWaterMark: 4,
      onPause: () => events.push('pause'),
      onResume: () => events.push('resume'),
    })
    for (let i = 0; i < 4; i++) q.push(i)
    expect(events).toEqual(['pause'])
    const it = q[Symbol.asyncIterator]()
    await it.next()
    await it.next()
    await it.next() // below hwm/2 → resume
    expect(events).toEqual(['pause', 'resume'])
  })

  it('ignores pushes after end', async () => {
    const q = new AsyncQueue<number>()
    q.end()
    q.push(1)
    expect(await collect(q)).toEqual([])
  })
})
