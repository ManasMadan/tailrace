# webhook-receiver

An HTTP receiver for `@walcast/sink-webhook`, Node stdlib only. It verifies the `X-Walcast-Signature` HMAC with `verifySignature` (constant-time, against the raw request body), deduplicates on `event.id` — delivery is at-least-once, ids are stable across redeliveries — and only returns 2xx once the batch is processed. A non-2xx (including the 401 for a bad signature) makes walcast retry the batch with backoff.

## Run

```sh
export WALCAST_SECRET=change-me
npm start
```

## Matching daemon config

Point the walcast daemon at it in `walcast.config.json`:

```json
{
  "sinks": [
    {
      "use": "@walcast/sink-webhook",
      "config": { "url": "http://localhost:9799/hook", "secret": "change-me" }
    }
  ]
}
```
