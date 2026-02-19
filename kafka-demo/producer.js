const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'restaurante-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

const pedidos = [
  { id: 1, cliente: 'Juan',   producto: 'Pizza Margarita', monto: 1500 },
  { id: 2, cliente: 'María',  producto: 'Hamburguesa',     monto: 1200 },
  { id: 3, cliente: 'Carlos', producto: 'Sushi x10',       monto: 3500 },
  { id: 4, cliente: 'Ana',    producto: 'Empanadas x6',    monto: 900  },
  { id: 5, cliente: 'Luis',   producto: 'Milanesa',        monto: 1100 },
]

const publicarPedidos = async () => {
  await producer.connect()
  console.log('Producer conectado\n')

  for (const pedido of pedidos) {
    await producer.send({
      topic: 'pedidos',
      messages: [
        {
          key: String(pedido.id),
          value: JSON.stringify({
            ...pedido,
            estado: 'NUEVO',
            intentos: 0,
            fechaCreacion: new Date().toISOString()
          })
        }
      ]
    })
    console.log(`Pedido publicado → ID: ${pedido.id} | Cliente: ${pedido.cliente} | Producto: ${pedido.producto}`)
  }

  console.log('\nTodos los pedidos publicados en el topic "pedidos"')
  await producer.disconnect()
}

publicarPedidos().catch(console.error)
