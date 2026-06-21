export { Walcast, type WalcastOptions } from '@/walcast'
export type { ChangeEvent } from '@/events'
export { parseLsn, formatLsn, type Lsn } from '@/lsn'
export { PgoutputDecoder } from '@/pgoutput/decoder'
export {
  UNCHANGED_TOAST,
  type PgoutputMessage,
  type RelationMessage,
  type BeginMessage,
  type CommitMessage,
  type InsertMessage,
  type UpdateMessage,
  type DeleteMessage,
  type TruncateMessage,
  type Tuple,
  type ColumnValue,
} from '@/pgoutput/messages'
export { ReplicationStream, type ReplicationStreamOptions } from '@/replication/stream'
export {
  parseReplicationMessage,
  buildStandbyStatusUpdate,
  type XLogData,
  type PrimaryKeepalive,
  type ReplicationMessage,
} from '@/replication/protocol'
export { ensureSetup, inspectSetup, teardown, type SetupOptions, type SetupStatus } from '@/setup'
export { compareEvents } from '@/events'
export {
  SinkEngine,
  type SinkEngineOptions,
  type EngineSinkSpec,
  type EngineStats,
  type SinkStats,
} from '@/engine/engine'
export { CheckpointStore, type SinkCheckpoint } from '@/engine/checkpoints'
export { serve } from '@/daemon/serve'
export {
  loadConfig,
  NO_SINKS_ERROR,
  type DaemonConfig,
  type SinkConfigEntry,
} from '@/daemon/config'
export { createLogger } from '@/logger'
