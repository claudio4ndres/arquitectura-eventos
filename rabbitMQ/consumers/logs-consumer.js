const { conectar, EXCHANGES, QUEUES } = require('../config')

// El servicio de logs NUNCA falla ni rechaza mensajes
// Su único trabajo es registrar todo lo que pasa
const registrarLog = (pedido) => {
  const log = {
    timestamp:  new Date().toISOString(),
    nivel:      'INFO',
    servicio:   'ecommerce',
    evento:     'PEDIDO_RECIBIDO',
    pedidoId:   pedido.id,
    cliente:    pedido.cliente,
    monto:      pedido.monto,
    metodoPago: pedido.metodoPago,
  }

  // En producción esto iría a: Elasticsearch, Datadog, CloudWatch, etc.
  console.log(`\n [LOGS] Registrando → ${JSON.stringify(log, null, 2)}`)
  return log
}

const iniciar = async () => {
  const { channel } = await conectar()

  await channel.assertExchange(EXCHANGES.PEDIDOS, 'fanout', { durable: true })
  await channel.assertQueue(QUEUES.LOGS, { durable: true })
  await channel.bindQueue(QUEUES.LOGS, EXCHANGES.PEDIDOS, '')

  // Los logs pueden procesar varios mensajes a la vez
  channel.prefetch(5)

  console.log(' Servicio LOGS escuchando...\n')

  channel.consume(QUEUES.LOGS, async (msg) => {
    if (!msg) return

    const pedido = JSON.parse(msg.content.toString())
    registrarLog(pedido)

    channel.ack(msg)  // siempre ack — los logs nunca se rechazan
  })
}

iniciar().catch(console.error)
