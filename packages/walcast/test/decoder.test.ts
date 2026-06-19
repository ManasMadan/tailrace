import { describe, expect, it } from 'vitest'
import { PgoutputDecoder } from '@/pgoutput/decoder'
import { UNCHANGED_TOAST } from '@/pgoutput/messages'
import * as fx from './fixtures'

const USERS_COLS = [
  { name: 'id', typeOid: 23, key: true },
  { name: 'email', typeOid: 25 },
  { name: 'active', typeOid: 16 },
  { name: 'bio', typeOid: 25 },
]

function decoderWithUsers(): PgoutputDecoder {
  const d = new PgoutputDecoder()
  d.decode(fx.relation(16385, 'public', 'users', 'd', USERS_COLS))
  return d
}

describe('PgoutputDecoder', () => {
  it('decodes Begin with commit LSN, timestamp, and xid', () => {
    const d = new PgoutputDecoder()
    // 2024-01-01T00:00:00Z in µs since the Postgres epoch (2000-01-01)
    const pgTime = (BigInt(Date.UTC(2024, 0, 1)) - 946_684_800_000n) * 1000n
    const msg = d.decode(fx.begin(0x1_a2b3c4n, pgTime, 777))
    expect(msg).toMatchObject({ tag: 'begin', commitLsn: 0x1_a2b3c4n, xid: 777 })
    expect(msg.tag === 'begin' && msg.commitTime.toISOString()).toBe('2024-01-01T00:00:00.000Z')
  })

  it('decodes Commit with commit and end LSNs', () => {
    const d = new PgoutputDecoder()
    const msg = d.decode(fx.commit(100n, 164n, 0n))
    expect(msg).toMatchObject({ tag: 'commit', commitLsn: 100n, endLsn: 164n })
  })

  it('decodes Origin', () => {
    const d = new PgoutputDecoder()
    const msg = d.decode(fx.origin(42n, 'upstream'))
    expect(msg).toMatchObject({ tag: 'origin', commitLsn: 42n, name: 'upstream' })
  })

  it('decodes Relation with replica identity and key flags', () => {
    const d = new PgoutputDecoder()
    const msg = d.decode(fx.relation(16385, 'public', 'users', 'f', USERS_COLS))
    expect(msg).toMatchObject({
      tag: 'relation',
      relationId: 16385,
      schema: 'public',
      name: 'users',
      replicaIdentity: 'f',
    })
    expect(msg.tag === 'relation' && msg.columns).toEqual([
      { flags: 1, name: 'id', typeOid: 23, typeMod: -1 },
      { flags: 0, name: 'email', typeOid: 25, typeMod: -1 },
      { flags: 0, name: 'active', typeOid: 16, typeMod: -1 },
      { flags: 0, name: 'bio', typeOid: 25, typeMod: -1 },
    ])
  })

  it('maps an empty relation namespace to pg_catalog', () => {
    const d = new PgoutputDecoder()
    const msg = d.decode(fx.relation(1, '', 'pg_thing', 'd', [{ name: 'x', typeOid: 25 }]))
    expect(msg.tag === 'relation' && msg.schema).toBe('pg_catalog')
  })

  it('decodes Type', () => {
    const d = new PgoutputDecoder()
    const msg = d.decode(fx.typeMessage(99999, 'public', 'mood'))
    expect(msg).toMatchObject({ tag: 'type', typeOid: 99999, schema: 'public', name: 'mood' })
  })

  it('decodes Insert into a named tuple with type conversion', () => {
    const d = decoderWithUsers()
    const msg = d.decode(fx.insert(16385, ['7', 'a@b.c', 't', 'hello']))
    expect(msg).toMatchObject({
      tag: 'insert',
      new: { id: 7, email: 'a@b.c', active: true, bio: 'hello' },
    })
  })

  it('decodes null columns', () => {
    const d = decoderWithUsers()
    const msg = d.decode(fx.insert(16385, ['7', null, 'f', null]))
    expect(msg.tag === 'insert' && msg.new).toEqual({
      id: 7,
      email: null,
      active: false,
      bio: null,
    })
  })

  it('marks unchanged TOAST columns with the sentinel', () => {
    const d = decoderWithUsers()
    const msg = d.decode(fx.update(16385, ['7', 'new@b.c', 't', { toast: true }]))
    expect(msg.tag === 'update' && msg.new).toEqual({
      id: 7,
      email: 'new@b.c',
      active: true,
      bio: UNCHANGED_TOAST,
    })
  })

  it('decodes Update without an old tuple (default replica identity)', () => {
    const d = decoderWithUsers()
    const msg = d.decode(fx.update(16385, ['7', 'x@y.z', 't', 'b']))
    expect(msg).toMatchObject({ tag: 'update', old: null })
  })

  it('decodes Update with a key old tuple (K)', () => {
    const d = decoderWithUsers()
    const msg = d.decode(
      fx.update(16385, ['8', 'x@y.z', 't', 'b'], { kind: 'K', values: ['7', null, null, null] }),
    )
    expect(msg.tag === 'update' && msg.old).toEqual({ id: 7, email: null, active: null, bio: null })
    expect(msg.tag === 'update' && msg.new).toMatchObject({ id: 8 })
  })

  it('decodes Update with a full old tuple (O, REPLICA IDENTITY FULL)', () => {
    const d = decoderWithUsers()
    const msg = d.decode(
      fx.update(16385, ['7', 'new@b.c', 'f', 'bio2'], {
        kind: 'O',
        values: ['7', 'old@b.c', 't', 'bio1'],
      }),
    )
    expect(msg.tag === 'update' && msg.old).toEqual({
      id: 7,
      email: 'old@b.c',
      active: true,
      bio: 'bio1',
    })
  })

  it('decodes Delete with key columns', () => {
    const d = decoderWithUsers()
    const msg = d.decode(fx.deleteMsg(16385, 'K', ['7', null, null, null]))
    expect(msg.tag === 'delete' && msg.old).toMatchObject({ id: 7 })
  })

  it('decodes Truncate with options and multiple relations', () => {
    const d = decoderWithUsers()
    d.decode(fx.relation(16400, 'public', 'orders', 'd', [{ name: 'id', typeOid: 23, key: true }]))
    const msg = d.decode(fx.truncate([16385, 16400], { cascade: true, restartIdentity: true }))
    expect(msg.tag === 'truncate' && msg.relations.map((r) => r.name)).toEqual(['users', 'orders'])
    expect(msg).toMatchObject({ cascade: true, restartIdentity: true })
  })

  it('converts json/jsonb and floats, leaves numeric/int8 as strings', () => {
    const d = new PgoutputDecoder()
    d.decode(
      fx.relation(20000, 'public', 'm', 'd', [
        { name: 'big', typeOid: 20 },
        { name: 'price', typeOid: 1700 },
        { name: 'ratio', typeOid: 701 },
        { name: 'meta', typeOid: 3802 },
      ]),
    )
    const msg = d.decode(fx.insert(20000, ['9007199254740993', '19.99', '0.5', '{"a":[1,2]}']))
    expect(msg.tag === 'insert' && msg.new).toEqual({
      big: '9007199254740993', // beyond MAX_SAFE_INTEGER — stays exact
      price: '19.99',
      ratio: 0.5,
      meta: { a: [1, 2] },
    })
  })

  it('throws on a change for a relation it has never seen', () => {
    const d = new PgoutputDecoder()
    expect(() => d.decode(fx.insert(555, ['1']))).toThrow(/unknown relation 555/)
  })

  it('throws on an unknown message tag', () => {
    const d = new PgoutputDecoder()
    expect(() => d.decode(Buffer.from('Zjunk'))).toThrow(/unknown message tag 'Z'/)
  })
})
