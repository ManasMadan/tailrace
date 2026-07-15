# Contributing to walcast

Thanks for wanting to help. This document covers how to get a dev environment
running, how the repo is laid out, and how to contribute — including the most
common contribution of all: a new sink plugin (spoiler: it doesn't go in this
repo, and that's a feature).

## Dev setup

You need:

- **Node >= 20** (there's an `.nvmrc`)
- **pnpm** (`corepack enable` is the easiest way; the version is pinned in
  `package.json`'s `packageManager` field)
- **Docker** — only for the integration tests; everything else works without it

```bash
git clone https://github.com/ManasMadan/walcast.git
cd walcast
pnpm install
pnpm build
pnpm test
```

## Repo layout

```
packages/walcast        the core: pgoutput decoder, replication client,
                         async-iterator API, and the daemon (engine + admin API)
packages/plugin-kit      @walcast/plugin-kit — the Sink contract (types) and
                         verifySink, the conformance harness every sink must pass
packages/sink-webhook    reference sink: durable HTTP POST, HMAC-signed
packages/sink-sse        ephemeral Server-Sent Events sink
packages/sink-kafka      durable Kafka sink, exactly-once into the topic
packages/sink-grpc       durable gRPC push sink
packages/typegen-prisma  generate typed events from a Prisma schema
packages/integration-tests  end-to-end tests against real Postgres/Kafka
apps/ui                  the daemon dashboard (static assets, no CDN)
examples/                runnable consumer examples
```

If you're wondering _why_ something is the way it is, check
[DECISIONS.md](./DECISIONS.md) first — non-obvious choices are recorded there,
and a PR that reverses one of them should argue with the recorded reasoning.

## Running tests

```bash
pnpm -r test          # everything
pnpm --filter walcast test   # one package
```

Unit tests run anywhere. The integration tests start throwaway
`postgres:16` (with `wal_level=logical`) and Kafka KRaft containers via
docker, and **skip themselves when docker isn't available** — so a red
integration suite is real, and a green one on a docker-less machine proves
less than you'd hope. Run them before sending anything that touches
replication, the engine, or a sink.

## Lint, format, commits

- `pnpm lint` (ESLint, type-checked rules) and `pnpm format` (Prettier).
  A husky pre-commit hook runs lint-staged, so staged files get fixed
  automatically — if the hook rewrites something, just re-stage it.
- Commit messages are **conventional commits**, enforced by commitlint at
  commit time: `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`, with an
  optional scope like `feat(sink-kafka): ...`.

## Versioning: changesets

Any change that should ship in a release needs a changeset:

```bash
pnpm changeset
```

Pick the affected packages and a bump (patch/minor — we're pre-1.0, so
breaking changes are minors), write one sentence a user would want to read in
a changelog. Docs-only and CI-only changes don't need one.

All packages version in lockstep (a `fixed` changesets group), so one tag
describes a release of everything. Releases are tag-triggered and cut by a
maintainer:

```bash
pnpm changeset version                          # apply pending changesets
git commit -am "chore(release): v0.2.0"
git tag -s v0.2.0 -m v0.2.0                     # signed, v-prefixed semver
git push origin master v0.2.0
```

Pushing the tag runs the release workflow: packaging guards, npm publish
with provenance for every public package, and a GitHub release with notes
from the changelog.

## Contributing a sink plugin

Walcast's core ships zero sinks on purpose — everything that transports
events is a plugin, and **community sinks live in their own repositories**,
not in this monorepo. This repo only carries the official `@walcast/*`
sinks. Your sink gets listed on the community sinks page in the docs, and you
keep ownership, release cadence, and issue tracker.

### Naming

- Community sinks: **`walcast-sink-<name>`** (e.g. `walcast-sink-clickhouse`)
- The **`@walcast/*`** scope is reserved for official packages — don't
  publish under it.

### How a sink works

A sink package's **default export is a factory `(config) => Sink`**. The
daemon resolves your package from the _user's_ project `node_modules` (see
`packages/walcast/src/daemon/serve.ts`), calls the factory with the
`config` object from their walcast configuration, then drives the `Sink`
contract from `@walcast/plugin-kit`:

- `init(ctx)` once — read `ctx.config`, log, open connections, optionally
  register HTTP routes; `ctx.resumeLsn` tells a durable sink where it left off
- `deliver(batch)` with batches in strict commit order — **throw to make the
  engine retry** (durable sinks are retried with backoff and never skipped)
- `close()` on shutdown
- Delivery is at-least-once: the same batch may arrive again with identical
  event ids. Be idempotent or tolerate duplicates.

The fastest way to start is the template in
[`templates/plugin/`](./templates/plugin/) — a complete, working NDJSON file
sink with the contract explained in comments, plus a conformance test. Copy
it, rename, replace the transport. `@walcast/sink-webhook` is the reference
implementation if you want to see a real one.

### Checklist for listing your sink

When you open a PR adding your sink to the community sinks docs page, it
should:

- [ ] pass `verifySink` from `@walcast/plugin-kit` in its own test suite
- [ ] declare `durability` correctly — `'durable'` only if a failed `deliver`
      throws and redelivery is safe; `'ephemeral'` if delivery is best-effort
- [ ] have a README with a complete config reference (every key, type,
      default)
- [ ] follow semver
- [ ] include `"walcast"` and `"walcast-sink"` in `package.json` keywords so
      it's discoverable on npm

That's it. Small PRs merge fastest; if you're planning something large, open
an issue first so we can talk it through.
