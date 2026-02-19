const { conectar, EXCHANGES } = require('./config')

const pedidos = [
  { id: 101, cliente: 'Juan Pérez',    email: 'juan@mail.com',  producto: 'Notebook',    monto: 85000, metodoPago: 'tarjeta' },
  { id: 102, cliente: 'María García',  email: 'maria@mail.com', producto: 'Auriculares', monto: 12000, metodoPago: 'efectivo' },
  { id: 103, cliente: 'Carlos López',  email: 'carlos@mail.com',producto: 'Monitor',     monto: 65000, metodoPago: 'tarjeta' },
  { id: 104, cliente: 'Ana Martínez',  email: 'ana@mail.com',   producto: 'Teclado',     monto: 8000,  metodoPago: 'debito' },
]

const publicarPedidos = async () => {
  const { conn, channel } = await conectar()

  // Declarar exchange tipo fanout
  // fanout = el mensaje se manda a TODAS las colas vinculadas
  await channel.assertExchange(EXCHANGES.PEDIDOS, 'fanout', { durable: true })

  console.log(' Producer conectado al exchange:', EXCHANGES.PEDIDOS)
  console.log(' Tipo: FANOUT — cada pedido llegará a los 4 servicios\n')

  for (const pedido of pedidos) {
    const mensaje = {
      ...pedido,
      estado: 'NUEVO',
      fecha: new Date().toISOString()
    }

    // Con fanout, la routingKey se ignora (se manda a todos)
    channel.publish(
      EXCHANGES.PEDIDOS,
      '',                                    // routingKey vacía en fanout
      Buffer.from(JSON.stringify(mensaje)),
      { persistent: true }                   // el mensaje sobrevive reinicios
    )

    console.log(` Pedido publicado → ID: ${pedido.id} | ${pedido.cliente} | ${pedido.producto} | $${pedido.monto}`)
  }

  console.log('\n Todos los pedidos publicados — los 4 servicios deberían recibirlos')

  setTimeout(async () => {
    await channel.close()
    await conn.close()
  }, 500)
}

publicarPedidos().catch(console.error)
