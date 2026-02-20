const { crearCliente, TOPICS } = require('../config')

// En Pulsar, cada subscripción con nombre diferente recibe TODOS los mensajes
// Esto es igual a Kafka — cada "grupo" tiene su propia copia del stream
const SUBSCRIPTION = 'analytics-service'

const iniciar = async () => {
  const client   = crearCliente()
  const consumer = await client.subscribe({
    topic:             TOPICS.EVENTOS_USUARIO,
    subscription:      SUBSCRIPTION,
    subscriptionType:  'Exclusive',   // un solo consumer activo (modo streaming)
  })

  console.log(' Servicio ANALYTICS escuchando...')
  console.log(`   Subscription: ${SUBSCRIPTION}`)
  console.log('   Tipo: Exclusive (streaming) — recibe todos los eventos\n')

  // Métricas acumuladas
  const metricas = {}

  while (true) {
    const msg    = await consumer.receive()
    const evento = JSON.parse(msg.getData().toString())

    // Acumular métricas por tipo de evento
    metricas[evento.tipo] = (metricas[evento.tipo] || 0) + 1

    console.log(`\n [ANALYTICS] Evento recibido`)
    console.log(`   Usuario: ${evento.nombre} (${evento.plan})`)
    console.log(`   Tipo:    ${evento.tipo}`)
    console.log(`   Contenido: ${evento.contenido}`)
    console.log(`    Acumulado: ${JSON.stringify(metricas)}`)

    await consumer.acknowledge(msg)  // equivalente al ack de RabbitMQ
  }
}

iniciar().catch(console.error)
