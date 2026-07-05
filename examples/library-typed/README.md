# library-typed

Library mode with typed events. `@walcast/typegen-prisma` reads your Prisma schema and generates a self-contained types file (no runtime imports) with an `isChange(event, 'users')` type guard that narrows `event.after` / `event.before` to the row shape of that table.

The generated types are honest about what pgoutput actually delivers: `BigInt` and `Decimal` arrive as strings, `DateTime` as Postgres text timestamps, `Json` as parsed JSON. `src/walcast-types.ts` is committed so the example runs as-is; regenerate after editing the schema:

```sh
npm run generate
```

## Prerequisites

A Postgres with logical replication enabled (`wal_level=logical`):

```sh
docker run -d --name walcast-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16-alpine -c wal_level=logical
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
```

And the table from the schema (it must exist before `setup()` creates the publication):

```sql
CREATE TABLE users (
  id bigserial PRIMARY KEY,
  email text NOT NULL,
  name text,
  metadata jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);
```

## Run

Requires Node 22.6+ (`--experimental-strip-types`; a no-op flag on Node 23+ where type stripping is on by default).

```sh
npm start
```

Then insert a row:

```sql
INSERT INTO users (email, metadata) VALUES ('ada@example.com', '{"plan":"pro"}');
```
