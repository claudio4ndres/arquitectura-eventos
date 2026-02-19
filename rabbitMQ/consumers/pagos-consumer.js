const { conectar, EXCHANGES, QUEUES } = require('../config')

const procesarPago = async (pedido) => {
  await new Promise(resolve => setTimeout(resolve, 400))

  // Simulamos que efectivo requiere validación manual
  if (pedido.metodoPago === 'efectivo') {
    throw new Error(`Pago en efectivo requiere validación manual`)
  }

  const aprobado = Math.random() > 0.1  // 90% de aprobación
  if (!aprobado) throw new Error('Pago rechazado por la entidad bancaria')

  return {
    transaccionId: `TXN-${Date.now()}`,
    metodoPago: pedido.metodoPago,
    monto: pedido.monto
  }
}

const iniciar = async () => {
  const { channel } = await conectar()

  await channel.assertExchange(EXCHANGES.PEDIDOS, 'fanout', { durable: true })
  await channel.assertQueue(QUEUES.PAGOS, { durable: true })
  await channel.bindQueue(QUEUES.PAGOS, EXCHANGES.PEDIDOS, '')

  channel.prefetch(1)

  console.log(' Servicio PAGOS escuchando...\n')

  channel.consume(QUEUES.PAGOS, async (msg) => {
    if (!msg) return

    const pedido = JSON.parse(msg.content.toString())
    console.log(`\n[PAGOS] Procesando → ID: ${pedido.id} | $${pedido.monto} | ${pedido.metodoPago}`)

    try {
      const resultado = await procesarPago(pedido)
      console.log(`    Pago aprobado → Transacción: ${resultado.transaccionId}`)

      channel.ack(msg)

    } catch (error) {
      console.log(`    Pago fallido: ${error.message}`)
      channel.nack(msg, false, false)  // false = no reencolar (pago fallido es definitivo)
    }
  })
}

iniciar().catch(console.error)
