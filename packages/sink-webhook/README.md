# @walcast/sink-webhook

Durable HTTP delivery for [walcast](https://github.com/ManasMadan/walcast):
ordered batches POSTed as JSON arrays, signed with HMAC-SHA256. Node stdlib
only — zero dependencies.

```bash
npm install walcast @walcast/sink-webhook
```

```jsonc
// walcast.config.json
{
  "sinks": [
    {
      "use": "@walcast/sink-webhook",
      "config": {
        "url": "https://example.com/hooks/walcast",
        "secret": "shared-secret", // optional; enables X-Walcast-Signature
        "headers": { "authorization": "Bearer ..." }, // optional extras
        "timeoutMs": 30000,
      },
    },
  ],
}
```

Semantics: at-least-once, strictly in order. Any non-2xx response makes the
engine retry with exponential backoff and jitter; after `maxAttempts` the
sink pauses with its last error (resumable from the API/UI) and never skips.
Deduplicate on `event.id` — it is identical across redeliveries.

Receivers verify signatures with the exported helper:

```ts
import { verifySignature } from '@walcast/sink-webhook'

verifySignature(rawBody, secret, req.headers['x-walcast-signature'])
```

Docs: https://walcast.mmadan.in/guide/sinks/webhook

## License

MIT
