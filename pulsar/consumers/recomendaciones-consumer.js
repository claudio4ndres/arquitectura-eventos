const { crearCliente, TOPICS } = require('../config')

// Subscription DIFERENTE = recibe los mismos mensajes que analytics
// Así funciona Pulsar en modo streaming — cada servicio tiene su propia "vista" del stream
const SUBSCRIPTION = 'recomendaciones-service'

const generarRecomendacion = (evento) => {
  const recomendaciones = {
    'REPRODUCCION_INICIADA':  ['Ozark', 'Narcos', 'Money Heist'],
    'CONTENIDO_COMPLETADO':   ['The Crown', 'Peaky Blinders', 'Dark'],
    'BUSQUEDA_REALIZADA':     ['Stranger Things', 'Black Mirror', 'Mindhunter'],
    'CALIFICACION_DADA':      ['Better Call Saul', 'Succession', 'The Wire'],
    'REPRODUCCION_PAUSADA':   [],
  }
  return recomendaciones[evento.tipo] || []
}

const iniciar = async () => {
  const client   = crearCliente()
  const consumer = await client.subscribe({
    topic:            TOPICS.EVENTOS_USUARIO,
    subscription:     SUBSCRIPTION,
    subscriptionType: 'Exclusive',
  })

  console.log(' Servicio RECOMENDACIONES escuchando...')
  console.log(`   Subscription: ${SUBSCRIPTION}`)
  console.log('   Recibe los MISMOS eventos que analytics (independientemente)\n')

  while (true) {
    const msg    = await consumer.receive()
    const evento = JSON.parse(msg.getData().toString())

    const recomendados = generarRecomendacion(evento)

    if (recomendados.length > 0) {
      console.log(`\n [RECOMENDACIONES] Para ${evento.nombre}`)
      console.log(`   Basado en: ${evento.tipo} → "${evento.contenido}"`)
      console.log(`   Sugerencias: ${recomendados.join(', ')}`)
    }

    await consumer.acknowledge(msg)
  }
}

iniciar().catch(console.error)
