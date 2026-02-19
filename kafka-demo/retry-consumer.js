const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'retry-procesador',
  brokers: ['localhost:9092']
})

// Mismo groupId que el consumer principal → Kafka los trata como el mismo grupo
const consumer = kafka.consumer({ groupId: 'procesadores' })
const producer = kafka.producer()

const MAX_INTENTOS = 3

// En retry, simulamos que a veces el problema ya se resolvió
const procesarPedidoRetry = async (pedido) => {
  // Simulamos que el 2do intento falla pero el 3ro funciona
  if (pedido.intentos < 2) {
    throw new Error(`Servicio externo no disponible (intento ${pedido.intentos + 1})`)
  }
  await new Promise(resolve => setTimeout(resolve, 300))
}

const iniciar = async () => {
  await producer.connect()
  await consumer.connect()
  console.log(' Retry consumer listo, escuchando "pedidos-retry"...\n')

  await consumer.subscribe({ topic: 'pedidos-retry', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ message }) => {
      const pedido = JSON.parse(message.value.toString())

      console.log(`\n Reintentando pedido → ID: ${pedido.id} | Cliente: ${pedido.cliente}`)
      console.log(`   Intento: ${pedido.intentos + 1}/${MAX_INTENTOS}`)
      console.log(`   Último error: ${pedido.ultimoError}`)

      try {
        await procesarPedidoRetry(pedido)
        console.log(`   Pedido ${pedido.id} procesado en el reintento!`)

      } catch (error) {
        console.log(`   Falló nuevamente: ${error.message}`)

        const intentosActualizados = pedido.intentos + 1

        if (intentosActualizados < MAX_INTENTOS) {
          await producer.send({
            topic: 'pedidos-retry',
            messages: [{
              key: String(pedido.id),
              value: JSON.stringify({
                ...pedido,
                intentos: intentosActualizados,
                ultimoError: error.message,
                fechaRetry: new Date().toISOString()
              })
            }]
          })
          console.log(`  Reencolado nuevamente (intento ${intentosActualizados}/${MAX_INTENTOS})`)

        } else {
          await producer.send({
            topic: 'pedidos-dlq',
            messages: [{
              key: String(pedido.id),
              value: JSON.stringify({
                ...pedido,
                intentos: intentosActualizados,
                errorFinal: error.message,
                fechaDLQ: new Date().toISOString()
              })
            }]
          })
          console.log(`  Enviado a DLQ — agotó todos los intentos`)
        }
      }
    }
  })
}

iniciar().catch(console.error)
