const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'dlq-monitor',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'dlq-monitores' })

const iniciar = async () => {
  await consumer.connect()
  console.log('DLQ Monitor activo, escuchando "pedidos-dlq"...')
  console.log('   (Estos son pedidos que fallaron y necesitan revisión manual)\n')

  await consumer.subscribe({ topic: 'pedidos-dlq', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ message }) => {
      const pedido = JSON.parse(message.value.toString())

      console.log('\n============ ALERTA DLQ ============')
      console.log(`   ID Pedido:  ${pedido.id}`)
      console.log(`   Cliente:    ${pedido.cliente}`)
      console.log(`   Producto:   ${pedido.producto}`)
      console.log(`   Monto:      $${pedido.monto}`)
      console.log(`   Intentos:   ${pedido.intentos}`)
      console.log(`   Error:      ${pedido.errorFinal}`)
      console.log(`   Fecha DLQ:  ${pedido.fechaDLQ}`)
      console.log('   Requiere intervención manual')
      console.log('=====================================\n')

      // Acá en producción podrías:
      // - Enviar un email/slack de alerta al equipo
      // - Guardar en una tabla "pedidos_fallidos" en la DB
      // - Crear un ticket en Jira automáticamente
    }
  })
}

iniciar().catch(console.error)
