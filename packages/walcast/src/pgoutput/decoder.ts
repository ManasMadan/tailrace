import { pgTimeToDate, readLsn } from '@/lsn'
import {
  UNCHANGED_TOAST,
  type ColumnValue,
  type PgoutputMessage,
  type RelationMessage,
  type Tuple,
} from '@/pgoutput/messages'

/**
 * Hand-written decoder for the `pgoutput` logical decoding plugin,
 * proto_version 1, text-format tuples.
 *
 * Stateful only in one way: Relation messages describe a table's shape and
 * arrive before the first change touching that table (and again whenever the
 * shape changes); we cache them by relation OID so Insert/Update/Delete
 * tuples can be materialized into named-column objects.
 */
export class PgoutputDecoder {
  private relations = new Map<number, RelationMessage>()

  decode(buf: Buffer): PgoutputMessage {
    const r = new Reader(buf)
    const tag = r.uint8()
    switch (tag) {
      case 0x42: {
        // 'B' Begin
        const commitLsn = r.lsn()
        const commitTime = pgTimeToDate(r.int64())
        const xid = r.uint32()
        return { tag: 'begin', commitLsn, commitTime, xid }
      }
      case 0x43: {
        // 'C' Commit
        r.uint8() // flags, currently unused (must be 0)
        const commitLsn = r.lsn()
        const endLsn = r.lsn()
        const commitTime = pgTimeToDate(r.int64())
        return { tag: 'commit', commitLsn, endLsn, commitTime }
      }
      case 0x4f: {
        // 'O' Origin
        const commitLsn = r.lsn()
        const name = r.cstring()
        return { tag: 'origin', commitLsn, name }
      }
      case 0x52: {
        // 'R' Relation
        const relationId = r.uint32()
        const schema = r.cstring() || 'pg_catalog'
        const name = r.cstring()
        const replicaIdentity = String.fromCharCode(r.uint8()) as RelationMessage['replicaIdentity']
        const ncols = r.uint16()
        const columns = []
        for (let i = 0; i < ncols; i++) {
          columns.push({
            flags: r.uint8(),
            name: r.cstring(),
            typeOid: r.uint32(),
            typeMod: r.int32(),
          })
        }
        const rel: RelationMessage = {
          tag: 'relation',
          relationId,
          schema,
          name,
          replicaIdentity,
          columns,
        }
        this.relations.set(relationId, rel)
        return rel
      }
      case 0x59: {
        // 'Y' Type
        const typeOid = r.uint32()
        const schema = r.cstring()
        const name = r.cstring()
        return { tag: 'type', typeOid, schema, name }
      }
      case 0x49: {
        // 'I' Insert
        const relation = this.relation(r.uint32())
        const kind = r.uint8()
        if (kind !== 0x4e) throw new Error(`pgoutput: insert tuple kind ${kind}, expected 'N'`)
        return { tag: 'insert', relation, new: this.tuple(r, relation) }
      }
      case 0x55: {
        // 'U' Update
        const relation = this.relation(r.uint32())
        let old: Tuple | null = null
        let kind = r.uint8()
        if (kind === 0x4b || kind === 0x4f) {
          // 'K' key columns | 'O' full old row
          old = this.tuple(r, relation)
          kind = r.uint8()
        }
        if (kind !== 0x4e) throw new Error(`pgoutput: update tuple kind ${kind}, expected 'N'`)
        return { tag: 'update', relation, old, new: this.tuple(r, relation) }
      }
      case 0x44: {
        // 'D' Delete
        const relation = this.relation(r.uint32())
        const kind = r.uint8()
        if (kind !== 0x4b && kind !== 0x4f) {
          throw new Error(`pgoutput: delete tuple kind ${kind}, expected 'K' or 'O'`)
        }
        return { tag: 'delete', relation, old: this.tuple(r, relation) }
      }
      case 0x54: {
        // 'T' Truncate
        const nrels = r.uint32()
        const options = r.uint8()
        const relations = []
        for (let i = 0; i < nrels; i++) relations.push(this.relation(r.uint32()))
        return {
          tag: 'truncate',
          cascade: (options & 1) !== 0,
          restartIdentity: (options & 2) !== 0,
          relations,
        }
      }
      default:
        throw new Error(
          `pgoutput: unknown message tag '${String.fromCharCode(tag)}' (0x${tag.toString(16)})`,
        )
    }
  }

  private relation(id: number): RelationMessage {
    const rel = this.relations.get(id)
    if (!rel) {
      throw new Error(
        `pgoutput: change for unknown relation ${id} — Relation message not seen on this connection`,
      )
    }
    return rel
  }

  /**
   * TupleData: int16 column count, then per column one of
   *   'n' NULL | 'u' unchanged TOAST | 't' int32 length + text bytes.
   */
  private tuple(r: Reader, rel: RelationMessage): Tuple {
    const ncols = r.uint16()
    const out: Tuple = {}
    for (let i = 0; i < ncols; i++) {
      const col = rel.columns[i]
      const name = col ? col.name : `_col${i}`
      const kind = r.uint8()
      switch (kind) {
        case 0x6e: // 'n'
          out[name] = null
          break
        case 0x75: // 'u'
          out[name] = UNCHANGED_TOAST
          break
        case 0x74: {
          // 't'
          const len = r.int32()
          const text = r.bytes(len).toString('utf8')
          out[name] = col ? convert(text, col.typeOid) : text
          break
        }
        default:
          throw new Error(`pgoutput: unknown tuple column kind 0x${kind.toString(16)}`)
      }
    }
    return out
  }
}

/**
 * Convert the text representation of common scalar types to JS values.
 * Anything not listed stays a string — predictable beats clever; numeric
 * and int8 stay strings to avoid silent precision loss.
 */
function convert(text: string, typeOid: number): ColumnValue {
  switch (typeOid) {
    case 16: // bool
      return text === 't'
    case 21: // int2
    case 23: // int4
    case 26: // oid
      return parseInt(text, 10)
    case 700: // float4
    case 701: // float8
      return parseFloat(text)
    case 114: // json
    case 3802: // jsonb
      return JSON.parse(text) as ColumnValue
    default:
      return text
  }
}

class Reader {
  private off = 0
  constructor(private buf: Buffer) {}

  uint8(): number {
    const v = this.buf.readUInt8(this.off)
    this.off += 1
    return v
  }
  uint16(): number {
    const v = this.buf.readUInt16BE(this.off)
    this.off += 2
    return v
  }
  uint32(): number {
    const v = this.buf.readUInt32BE(this.off)
    this.off += 4
    return v
  }
  int32(): number {
    const v = this.buf.readInt32BE(this.off)
    this.off += 4
    return v
  }
  int64(): bigint {
    const v = this.buf.readBigInt64BE(this.off)
    this.off += 8
    return v
  }
  lsn() {
    return readLsn(this.buf, (this.off += 8) - 8)
  }
  cstring(): string {
    const end = this.buf.indexOf(0, this.off)
    if (end === -1) throw new Error('pgoutput: unterminated cstring')
    const s = this.buf.toString('utf8', this.off, end)
    this.off = end + 1
    return s
  }
  bytes(n: number): Buffer {
    if (this.off + n > this.buf.length) throw new Error('pgoutput: truncated message')
    const b = this.buf.subarray(this.off, this.off + n)
    this.off += n
    return b
  }
}
