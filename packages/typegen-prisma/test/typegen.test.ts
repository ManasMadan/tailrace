import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import ts from 'typescript'
import { generate, parsePrismaSchema } from '@/index'

const SCHEMA = `
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

enum Role {
  ADMIN
  MEMBER
}

model User {
  id        Int      @id @default(autoincrement())
  email     String   @unique
  name      String?
  role      Role     @default(MEMBER)
  balance   Decimal
  views     BigInt
  createdAt DateTime @default(now()) @map("created_at")
  meta      Json?
  tags      String[]
  posts     Post[]

  @@map("users")
}

model Post {
  id       Int    @id @default(autoincrement())
  title    String
  author   User   @relation(fields: [authorId], references: [id])
  authorId Int    @map("author_id")
}
`

describe('@walcast/typegen-prisma', () => {
  it('parses models, enums, maps, and skips relations', () => {
    const { models, enums } = parsePrismaSchema(SCHEMA)
    expect(enums).toEqual([{ name: 'Role', values: ['ADMIN', 'MEMBER'] }])
    expect(models.map((m) => m.table)).toEqual(['users', 'Post'])

    const user = models[0]!
    const columns = Object.fromEntries(user.fields.map((f) => [f.column, f.tsType]))
    expect(columns).toEqual({
      id: 'number',
      email: 'string',
      name: 'string',
      role: 'Role',
      balance: 'string', // Decimal → string, no precision loss
      views: 'string', // BigInt → string
      created_at: 'string', // @map respected, DateTime is Postgres text
      meta: 'JsonValue',
      tags: 'string', // lists arrive as Postgres array literals
    })
    expect(user.fields.find((f) => f.column === 'name')?.optional).toBe(true)
    // relation fields don't exist as columns; the FK scalar does
    const post = models[1]!
    expect(post.fields.map((f) => f.column)).toEqual(['id', 'title', 'author_id'])
  })

  it('emits self-contained TypeScript that compiles with narrowing', () => {
    const generated = generate(SCHEMA)
    expect(generated).toContain(`'users': UserRow`)
    expect(generated).not.toMatch(/import /) // zero runtime imports

    const dir = mkdtempSync(join(tmpdir(), 'typegen-'))
    writeFileSync(join(dir, 'walcast-types.ts'), generated)
    writeFileSync(
      join(dir, 'usage.ts'),
      `import { isChange, type ChangeEvent, type Role } from './walcast-types'

const event = {} as ChangeEvent<unknown>
if (isChange(event, 'users')) {
  const email: string | undefined = event.after?.email
  const role: Role | undefined = event.after?.role
  const balance: string | undefined = event.after?.balance
  console.log(email, role, balance)
}
if (isChange(event, 'Post')) {
  const title: string | undefined = event.after?.title
  console.log(title)
}
`,
    )
    const program = ts.createProgram([join(dir, 'usage.ts')], {
      strict: true,
      noEmit: true,
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
    })
    const diagnostics = ts.getPreEmitDiagnostics(program)
    expect(diagnostics.map((d) => ts.flattenDiagnosticMessageText(d.messageText, '\n'))).toEqual([])
  })

  it('survives braces inside attribute strings without dropping fields', () => {
    const { models } = parsePrismaSchema(`model Settings {
  id       Int    @id
  config   Json   @default("{}")
  labels   String @default("{a}")
  trailing String
}

model After {
  id Int @id
}`)
    expect(models.map((m) => m.model)).toEqual(['Settings', 'After'])
    expect(models[0]!.fields.map((f) => f.column)).toEqual(['id', 'config', 'labels', 'trailing'])
  })

  it('uses @map values for enum members — events carry database values', () => {
    const { enums } = parsePrismaSchema(`enum Role {
  ADMIN  @map("admin")
  MEMBER
}`)
    expect(enums).toEqual([{ name: 'Role', values: ['admin', 'MEMBER'] }])
  })

  it('rejects nothing silently: unknown scalars fall back to string', () => {
    const { models } = parsePrismaSchema(`model T {
  id  Int @id
  geo Unsupported("geometry")
}`)
    expect(models[0]!.fields.map((f) => f.tsType)).toEqual(['number', 'string'])
  })
})
