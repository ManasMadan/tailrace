import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  test: {
    include: ['test/**/*.test.ts'],
    globalSetup: ['test/global-setup.ts'],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    // The engine and library integration suites share one Postgres;
    // parallel workers interfere (replication slots, walcast.sinks rows).
    fileParallelism: false,
  },
})
