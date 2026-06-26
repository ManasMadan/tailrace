import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    globalSetup: ['test/global-setup.ts'],
    testTimeout: 120_000,
    hookTimeout: 120_000,
    // Money tests spawn daemons bound to fixed slots; keep them serial.
    fileParallelism: false,
  },
})
