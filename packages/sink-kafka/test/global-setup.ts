import { execFileSync } from 'node:child_process'
import type { TestProject } from 'vitest/node'
import { Kafka, logLevel } from 'kafkajs'

/**
 * Single-broker KRaft Kafka for integration tests. Honors
 * WALCAST_TEST_KAFKA (e.g. a CI service); otherwise starts a throwaway
 * container. Without docker the suite skips itself.
 */

const CONTAINER = 'walcast-test-kafka'
const PORT = 19092
const BROKER = `127.0.0.1:${PORT}`

declare module 'vitest' {
  interface ProvidedContext {
    brokers: string[]
  }
}

function docker(...args: string[]): string {
  return execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
}

async function waitForKafka(broker: string, timeoutMs = 90_000): Promise<void> {
  const kafka = new Kafka({ clientId: 'readiness', brokers: [broker], logLevel: logLevel.NOTHING })
  const admin = kafka.admin()
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      await admin.connect()
      await admin.listTopics()
      await admin.disconnect()
      return
    } catch (err) {
      await admin.disconnect().catch(() => {})
      if (Date.now() > deadline) throw err
      await new Promise((r) => setTimeout(r, 750))
    }
  }
}

let startedContainer = false

export async function setup(project: TestProject): Promise<void> {
  if (process.env.WALCAST_TEST_KAFKA) {
    project.provide('brokers', process.env.WALCAST_TEST_KAFKA.split(','))
    return
  }
  try {
    const running = docker('ps', '-q', '--filter', `name=^${CONTAINER}$`).trim()
    if (!running) {
      docker('rm', '-f', CONTAINER)
      docker(
        'run',
        '-d',
        '--name',
        CONTAINER,
        '-p',
        `${PORT}:9092`,
        '-e',
        'KAFKA_NODE_ID=1',
        '-e',
        'KAFKA_PROCESS_ROLES=broker,controller',
        // Two data listeners: EXTERNAL is what the host (tests) reaches via
        // the mapped port; INTERNAL is what the broker's own transaction
        // coordinator dials — pointing it at the host mapping breaks EOS.
        '-e',
        'KAFKA_LISTENERS=EXTERNAL://0.0.0.0:9092,INTERNAL://localhost:29092,CONTROLLER://localhost:9093',
        '-e',
        `KAFKA_ADVERTISED_LISTENERS=EXTERNAL://${BROKER},INTERNAL://localhost:29092`,
        '-e',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
        '-e',
        'KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL',
        '-e',
        'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
        '-e',
        'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
        '-e',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
        '-e',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
        '-e',
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
        '-e',
        'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
        'apache/kafka:3.9.0',
      )
      startedContainer = true
    }
    await waitForKafka(BROKER)
    project.provide('brokers', [BROKER])
  } catch {
    console.warn('[sink-kafka tests] docker unavailable — integration tests will be skipped')
    project.provide('brokers', [])
  }
}

export function teardown(): void {
  if (startedContainer && !process.env.WALCAST_TEST_KEEP_KAFKA) {
    try {
      docker('rm', '-f', CONTAINER)
    } catch {
      /* already gone */
    }
  }
}
