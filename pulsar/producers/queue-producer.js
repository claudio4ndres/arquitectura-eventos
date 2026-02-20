const { crearCliente, TOPICS, inicializarNamespaces } = require('../config')

const notificaciones = [
  { usuarioId: 'u001', tipo: 'NUEVA_TEMPORADA',  mensaje: 'Breaking Bad tiene nueva temporada!',  canal: 'email' },
  { usuarioId: 'u002', tipo: 'RECOMENDACION',     mensaje: 'Te puede gustar: Dark',               canal: 'push'  },
  { usuarioId: 'u003', tipo: 'VENCIMIENTO_PLAN',  mensaje: 'Tu plan vence en 3 días',             canal: 'email' },
  { usuarioId: 'u001', tipo: 'CONTENIDO_NUEVO',   mensaje: 'Nuevo episodio disponible',           canal: 'push'  },
]

const publicarNotificaciones = async () => {
  await inicializarNamespaces()
  const client   = crearCliente()
  const producer = await client.createProducer({ topic: TOPICS.NOTIFICACIONES })

  console.log('📬 Producer QUEUE listo')
  console.log(`📡 Topic: ${TOPICS.NOTIFICACIONES}\n`)
  console.log('💡 Este topic usa modo QUEUE — cada notificación será procesada')
  console.log('   UNA SOLA VEZ por el consumer disponible (como RabbitMQ)\n')

  for (const notif of notificaciones) {
    const mensaje = { ...notif, id: `notif-${Date.now()}`, fecha: new Date().toISOString() }

    await producer.send({
      data: Buffer.from(JSON.stringify(mensaje)),
      properties: { canal: notif.canal, tipo: notif.tipo }
    })

    console.log(`📤 Notificación → Usuario: ${notif.usuarioId} | ${notif.tipo} | canal: ${notif.canal}`)
  }

  console.log('\n✅ Notificaciones publicadas en la queue')

  await producer.close()
  await client.close()
}

publicarNotificaciones().catch(console.error)
