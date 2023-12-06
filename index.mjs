
import Hyperswarm from 'hyperswarm'
import goodbye from 'graceful-goodbye'
import b4a from 'b4a'
import { createServer } from 'node:net'

import { parser, writeToStream } from 'mqtt-packet'
import aedes from 'aedes' 

const broker = aedes()
goodbye(() => broker.close())

const server = createServer(broker.handle)
goodbye(() => server.close())

const port = process.argv[2] ? parseInt(process.argv[2]) : 1883
server.listen(port, function () {
  console.log('server listening on port', port)
})

broker.on('clientError', function (client, err) {
  console.log('client error', client.id, err.message, err.stack)
})

broker.on('connectionError', function (client, err) {
  console.log('client error', client, err.message, err.stack)
})

broker.on('publish', function (packet, client) {
  if (client) {
    console.log('message from client', client.id, packet)
  }
})

broker.on('subscribe', function (subscriptions, client) {
  if (client) {
    console.log('subscribe from client', subscriptions, client.id)
  }
})

broker.on('client', function (client) {
  console.log('new client', client.id)
})

const swarm = new Hyperswarm()
goodbye(() => swarm.destroy())

// Keep track of all connections and console.log incoming data
const conns = []

swarm.on('connection', conn => {
  const name = b4a.toString(conn.remotePublicKey, 'hex')
  console.log('* got a connection from:', name, '*')
  conns.push(conn)
  conn.once('close', () => conns.splice(conns.indexOf(conn), 1))
  // Use a parser per hyper node connection
  const _parser = parser()
  _parser.on('packet', packet => {
    console.log('packet from hyperswarm', packet)
    broker.publish(packet)
  })
  conn.on('data', data => _parser.parse(data))
})

broker.on('publish', function (packet, client) {
  if (client) {
    console.log('writing published mqtt packet to hyperswarm nodes', client.id, packet)
    for (const conn of conns) {
      writeToStream(packet,conn)
    }
  }
})

// Join a topic
const topic = process.argv[3] ? b4a.from(process.argv[3], 'hex') : Buffer.alloc(32).fill('hello hypermqtt!')
const discovery = swarm.join(topic, { client: true, server: true })

// The flushed promise will resolve when the topic has been fully announced to the DHT
discovery.flushed().then(() => {
  console.log('joined topic:', b4a.toString(topic, 'hex'))
})
