const { crearCliente, TOPICS } = require('../config')

const iniciar = async () => {
  const client   = crearCliente()
  const consumer = await client.subscribe({
    topic:            TOPICS.NOTIFICACIONES,
    subscription:     'notificaciones-workers',
    // Shared = modo QUEUE — varios workers comparten la carga
    // cada mensaje va a UN SOLO worker (como RabbitMQ)
    subscriptionType: 'Shared',
  })

  console.log(' Servicio NOTIFICACIONES escuchando...')
  console.log('   Tipo: Shared (queue) — cada notificación se procesa UNA SOLA VEZ')
  console.log('   (podés correr múltiples instancias y Pulsar balancea la carga)\n')

  while (true) {
    const msg   = await consumer.receive()
    const notif = JSON.parse(msg.getData().toString())

    console.log(`\n [NOTIFICACIONES] Procesando`)
    console.log(`   Usuario:  ${notif.usuarioId}`)
    console.log(`   Tipo:     ${notif.tipo}`)
    console.log(`   Mensaje:  ${notif.mensaje}`)
    console.log(`   Canal:    ${notif.canal}`)

    // Simulamos envío según canal
    await new Promise(r => setTimeout(r, 200))

    if (notif.canal === 'email') {
      console.log(`    Email enviado correctamente`)
    } else if (notif.canal === 'push') {
      console.log(`    Push notification enviada`)
    }

    await consumer.acknowledge(msg)
  }
}

iniciar().catch(console.error)
