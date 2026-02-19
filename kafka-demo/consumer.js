const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'procesador-pedidos',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'procesadores' })
const producer = kafka.producer()

const MAX_INTENTOS = 3

// Simulamos que los pedidos con ID par fallan (para ver el retry en acción)
const procesarPedido = async (pedido) => {
  if (pedido.id % 2 === 0) {
    throw new Error(`Error de DB: no se pudo guardar el pedido ${pedido.id}`)
  }
  // Simular tiempo de procesamiento
  await new Promise(resolve => setTimeout(resolve, 300))
  console.log(`   Pedido procesado correctamente`)
}

const iniciar = async () => {
  await producer.connect()
  await consumer.connect()
  console.log('Consumer principal listo, escuchando "pedidos"...\n')

  await consumer.subscribe({ topic: 'pedidos', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const pedido = JSON.parse(message.value.toString())

      console.log(`\nPedido recibido → ID: ${pedido.id} | Cliente: ${pedido.cliente}`)
      console.log(`   Intento: ${pedido.intentos + 1}/${MAX_INTENTOS}`)

      try {
        await procesarPedido(pedido)
        console.log(`   Offset: ${message.offset} | Partition: ${partition}`)

      } catch (error) {
        console.log(`   Error: ${error.message}`)

        const intentosActualizados = pedido.intentos + 1

        if (intentosActualizados < MAX_INTENTOS) {
          // Todavía tiene intentos disponibles → va a retry
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
          console.log(`   Enviado a "pedidos-retry" (intento ${intentosActualizados}/${MAX_INTENTOS})`)

        } else {
          // Agotó los intentos → va a la Dead Letter Queue
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
          console.log(`   Enviado a "pedidos-dlq" — agotó ${MAX_INTENTOS} intentos`)
        }
      }
    }
  })
}

iniciar().catch(console.error)
