import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'walcast',
  description:
    'Postgres change data capture (logical replication) for Node. Zero-plugin library mode, plugin-driven daemon mode.',
  base: '/',
  lastUpdated: true,

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/why-walcast' },
      { text: 'Reference', link: '/reference/event-schema' },
      { text: 'GitHub', link: 'https://github.com/ManasMadan/walcast' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'Why walcast', link: '/guide/why-walcast' },
            { text: 'Quickstart: library', link: '/guide/quickstart-library' },
            { text: 'Quickstart: daemon', link: '/guide/quickstart-daemon' },
          ],
        },
        {
          text: 'Understanding walcast',
          items: [
            { text: 'Concepts: WAL, slots, LSNs', link: '/guide/concepts' },
            { text: 'Delivery guarantees', link: '/guide/delivery-guarantees' },
            { text: 'Typed events (Prisma)', link: '/guide/typed-events' },
          ],
        },
        {
          text: 'Sinks',
          items: [
            { text: 'Webhook', link: '/guide/sinks/webhook' },
            { text: 'SSE', link: '/guide/sinks/sse' },
            { text: 'Kafka', link: '/guide/sinks/kafka' },
            { text: 'gRPC', link: '/guide/sinks/grpc' },
            { text: 'Writing a sink', link: '/guide/writing-a-sink' },
            { text: 'Community sinks', link: '/guide/community-sinks' },
          ],
        },
        {
          text: 'Operations',
          items: [
            { text: 'Monitoring', link: '/guide/monitoring' },
            { text: 'Production checklist', link: '/guide/production-checklist' },
            { text: 'FAQ', link: '/guide/faq' },
            { text: 'Comparisons', link: '/guide/comparisons' },
          ],
        },
      ],
      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'Event schema', link: '/reference/event-schema' },
            { text: 'Configuration', link: '/reference/config' },
            { text: 'CLI', link: '/reference/cli' },
            { text: 'HTTP API', link: '/reference/http-api' },
            { text: 'gRPC contract', link: '/reference/grpc-contract' },
          ],
        },
      ],
    },

    socialLinks: [{ icon: 'github', link: 'https://github.com/ManasMadan/walcast' }],

    footer: {
      message: 'Released under the MIT License.',
    },

    search: {
      provider: 'local',
    },
  },
})
