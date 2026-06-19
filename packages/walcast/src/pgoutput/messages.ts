import type { Lsn } from '@/lsn'

/**
 * Decoded pgoutput (proto_version 1) messages. Field names follow the
 * protocol documentation:
 * https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
 */

export interface RelationColumn {
  name: string
  /** Bit 0 set = column is part of the replica identity key. */
  flags: number
  typeOid: number
  typeMod: number
}

export interface RelationMessage {
  tag: 'relation'
  relationId: number
  schema: string
  name: string
  /** d = default, n = nothing, f = full, i = index */
  replicaIdentity: 'd' | 'n' | 'f' | 'i'
  columns: RelationColumn[]
}

export interface BeginMessage {
  tag: 'begin'
  /** The transaction's final LSN, i.e. its commit LSN. */
  commitLsn: Lsn
  commitTime: Date
  xid: number
}

export interface CommitMessage {
  tag: 'commit'
  commitLsn: Lsn
  /** End LSN of the commit record — ack up to here to release the whole tx. */
  endLsn: Lsn
  commitTime: Date
}

export interface OriginMessage {
  tag: 'origin'
  commitLsn: Lsn
  name: string
}

export interface TypeMessage {
  tag: 'type'
  typeOid: number
  schema: string
  name: string
}

/**
 * Sentinel for a TOASTed column that was not changed by an UPDATE and is
 * therefore not present in the WAL record. A string (not a symbol) so it
 * survives JSON serialization through sinks; documented in the event schema.
 */
export const UNCHANGED_TOAST = '__walcast:unchanged_toast__'

export type ColumnValue = string | number | boolean | null | Record<string, unknown> | unknown[]

export type Tuple = Record<string, ColumnValue>

export interface InsertMessage {
  tag: 'insert'
  relation: RelationMessage
  new: Tuple
}

export interface UpdateMessage {
  tag: 'update'
  relation: RelationMessage
  /**
   * Present only when REPLICA IDENTITY is FULL (full old row, key kind 'O')
   * or when a key column changed (key columns only, kind 'K'). Otherwise null.
   */
  old: Tuple | null
  new: Tuple
}

export interface DeleteMessage {
  tag: 'delete'
  relation: RelationMessage
  /** Key columns ('K') or full old row ('O' under REPLICA IDENTITY FULL). */
  old: Tuple
}

export interface TruncateMessage {
  tag: 'truncate'
  cascade: boolean
  restartIdentity: boolean
  relations: RelationMessage[]
}

export type PgoutputMessage =
  | BeginMessage
  | CommitMessage
  | OriginMessage
  | RelationMessage
  | TypeMessage
  | InsertMessage
  | UpdateMessage
  | DeleteMessage
  | TruncateMessage
