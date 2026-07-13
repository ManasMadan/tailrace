// Seeds the compose demo: creates an orders table (REPLICA IDENTITY FULL so
// updates/deletes carry before-images), then generates scripted writes while
// you watch the live inspector.
//
//   docker compose up --build     # first
//   npm run demo
//   open http://127.0.0.1:7717/ui/?token=demo
import { Client } from 'pg'

const DSN = process.env.DEMO_DATABASE_URL ?? 'postgres://postgres:postgres@127.0.0.1:54320/postgres'
const ITEMS = ['espresso', 'flat white', 'cold brew', 'cortado', 'pour over', 'mocha']

const db = new Client({ connectionString: DSN })
await db.connect()

await db.query(`
  CREATE TABLE IF NOT EXISTS orders (
    id serial PRIMARY KEY,
    item text NOT NULL,
    qty int NOT NULL,
    total numeric(8,2) NOT NULL,
    status text NOT NULL DEFAULT 'placed'
  )
`)
await db.query(`ALTER TABLE orders REPLICA IDENTITY FULL`)

console.log('watching: http://127.0.0.1:7717/ui/?token=demo')
console.log('writing orders — ctrl-c to stop\n')

const rand = (n) => Math.floor(Math.random() * n)
let written = 0
for (;;) {
  const item = ITEMS[rand(ITEMS.length)]
  const { rows } = await db.query(
    `INSERT INTO orders (item, qty, total) VALUES ($1, $2, $3) RETURNING id`,
    [item, 1 + rand(3), (3.5 + rand(40) / 10).toFixed(2)],
  )
  const id = rows[0].id
  written++
  if (rand(3) === 0) {
    await db.query(`UPDATE orders SET status = 'brewing' WHERE id = $1`, [id])
  }
  if (rand(8) === 0) {
    await db.query(`DELETE FROM orders WHERE id = (SELECT min(id) FROM orders)`)
  }
  if (written % 25 === 0) console.log(`${written} orders written`)
  await new Promise((r) => setTimeout(r, 400))
}
