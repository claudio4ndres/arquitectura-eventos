const amqp = require('amqplib')

const RABBITMQ_URL = 'amqp://admin:admin123@localhost:5672'

// Nombres de exchanges y colas centralizados
const EXCHANGES = {
  PEDIDOS: 'ecommerce.pedidos',   // fanout — avisa a todos los servicios
  PAGOS:   'ecommerce.pagos',     // direct — solo al servicio de pagos
}

const QUEUES = {
  PEDIDOS:        'cola.pedidos',
  PAGOS:          'cola.pagos',
  NOTIFICACIONES: 'cola.notificaciones',
  LOGS:           'cola.logs',
}

const conectar = async () => {
  const conn    = await amqp.connect(RABBITMQ_URL)
  const channel = await conn.createChannel()
  return { conn, channel }
}

module.exports = { RABBITMQ_URL, EXCHANGES, QUEUES, conectar }
