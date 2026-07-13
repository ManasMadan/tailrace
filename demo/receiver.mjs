// Stand-in webhook consumer for the compose demo: logs each batch.
import { createServer } from 'node:http'

createServer((req, res) => {
  let body = ''
  req.on('data', (c) => (body += c))
  req.on('end', () => {
    const events = JSON.parse(body)
    console.log(`webhook batch: ${events.length} events, first id ${events[0]?.id}`)
    res.writeHead(200).end()
  })
}).listen(9799, () => console.log('demo webhook receiver on :9799'))
