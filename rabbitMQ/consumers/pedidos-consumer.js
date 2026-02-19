const { conectar, EXCHANGES, QUEUES } = require('../config')

const procesarPedido = async (pedido) => {
  // Simulamos guardar en DB
  await new Promise(resolve => setTimeout(resolve, 200))

  if (pedido.monto > 80000) {
    throw new Error(`Pedido ${pedido.id} supera el límite diario de compra`)
  }

  return { stock: 'reservado', almacen: 'Buenos Aires' }
}

const iniciar = async () => {
  const { channel } = await conectar()

  // Declarar el exchange (debe coincidir con el producer)
  await channel.assertExchange(EXCHANGES.PEDIDOS, 'fanout', { durable: true })

  // Declarar la cola
  await channel.assertQueue(QUEUES.PEDIDOS, { durable: true })

  // Vincular la cola al exchange — acá es donde decimos "quiero recibir mensajes de este exchange"
  await channel.bindQueue(QUEUES.PEDIDOS, EXCHANGES.PEDIDOS, '')

  // Procesar de a 1 mensaje por vez (no tomar otro hasta terminar el actual)
  channel.prefetch(1)

  console.log(' Servicio PEDIDOS escuchando...\n')

  channel.consume(QUEUES.PEDIDOS, async (msg) => {
    if (!msg) return

    const pedido = JSON.parse(msg.content.toString())
    console.log(`\n [PEDIDOS] Procesando → ID: ${pedido.id} | ${pedido.cliente}`)

    try {
      const resultado = await procesarPedido(pedido)
      console.log(`    Stock reservado en almacén: ${resultado.almacen}`)

      channel.ack(msg)   // ← confirmar que el mensaje fue procesado (se elimina de la cola)

    } catch (error) {
      console.log(`    Error: ${error.message}`)
      // nack = no acknowledgement → el mensaje vuelve a la cola para reintentarse
      channel.nack(msg, false, true)
    }
  })
}

iniciar().catch(console.error)
