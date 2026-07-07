import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { makeMockContext, makeTestEvents, verifySink, type ChangeEvent } from '@walcast/plugin-kit'
import factory from '../src/index.js'

// The conformance harness drives the whole contract: metadata, init,
// ordered delivery, redelivery, close idempotency. `collect` shows it what
// actually landed at the far end — here, the lines of the NDJSON file.
// Every sink's test suite should contain this call; it is what the
// community-sink checklist means by "passes verifySink".

let dir: string

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), 'walcast-sink-example-'))
})

afterAll(() => rm(dir, { recursive: true, force: true }))

async function readEvents(path: string): Promise<ChangeEvent[]> {
  const text = await readFile(path, 'utf8')
  return text
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as ChangeEvent)
}

describe('walcast-sink-example', () => {
  it('passes the conformance harness', async () => {
    const path = join(dir, 'conformance.ndjson')
    await verifySink(factory, {
      config: { path },
      collect: () => readEvents(path),
      // At-least-once: redelivery appends duplicate lines, which the
      // harness accepts (consumers dedupe on event.id). A sink with
      // exactly-once semantics into its transport would set
      // expectDedupe: true here.
    })
  })

  it('tolerates redelivery of an identical batch', async () => {
    const path = join(dir, 'redelivery.ndjson')
    const sink = factory({ path })
    await sink.init(makeMockContext({ path }))

    const batch = makeTestEvents(3)
    await sink.deliver(batch)
    // Crash-recovery case: the exact same batch again. Must not throw.
    await sink.deliver(batch)
    await sink.close()

    const seen = await readEvents(path)
    expect(seen).toHaveLength(6)
    // Ids are stable across redelivery — that is what lets consumers dedupe.
    expect(seen.slice(0, 3).map((e) => e.id)).toEqual(seen.slice(3).map((e) => e.id))
  })

  it('rejects a config without a path', () => {
    expect(() => factory({})).toThrow(/config\.path/)
  })
})
