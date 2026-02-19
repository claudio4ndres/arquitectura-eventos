const { conectar, EXCHANGES, QUEUES } = require('../config')

const enviarNotificacion = async (pedido) => {
  await new Promise(resolve => setTimeout(resolve, 150))

  // Simulamos envío de email y push notification
  const notificaciones = [
    ` Email enviado a: ${pedido.email}`,
    ` Push notification enviada al dispositivo del usuario`,
  ]

  return notificaciones
}

const iniciar = async () => {
  const { channel } = await conectar()

  await channel.assertExchange(EXCHANGES.PEDIDOS, 'fanout', { durable: true })
  await channel.assertQueue(QUEUES.NOTIFICACIONES, { durable: true })
  await channel.bindQueue(QUEUES.NOTIFICACIONES, EXCHANGES.PEDIDOS, '')

  channel.prefetch(1)

  console.log(' Servicio NOTIFICACIONES escuchando...\n')

  channel.consume(QUEUES.NOTIFICACIONES, async (msg) => {
    if (!msg) return

    const pedido = JSON.parse(msg.content.toString())
    console.log(`\n [NOTIF] Procesando → ID: ${pedido.id} | ${pedido.cliente}`)

    try {
      const notifs = await enviarNotificacion(pedido)
      notifs.forEach(n => console.log(`   ${n}`))
      console.log(`    Notificaciones enviadas correctamente`)

      channel.ack(msg)

    } catch (error) {
      console.log(`    Error enviando notificación: ${error.message}`)
      channel.nack(msg, false, true)
    }
  })
}

iniciar().catch(console.error)
