const { crearCliente, TOPICS, inicializarNamespaces } = require('../config')

// Simulamos eventos de una plataforma de streaming (tipo Netflix/Spotify)
const usuarios = [
  { id: 'u001', nombre: 'Juan',   plan: 'premium' },
  { id: 'u002', nombre: 'María',  plan: 'basic'   },
  { id: 'u003', nombre: 'Carlos', plan: 'premium' },
]

const tiposEvento = [
  { tipo: 'REPRODUCCION_INICIADA',  contenido: 'Breaking Bad S01E01' },
  { tipo: 'REPRODUCCION_PAUSADA',   contenido: 'Breaking Bad S01E01' },
  { tipo: 'BUSQUEDA_REALIZADA',     contenido: 'series de accion'    },
  { tipo: 'CONTENIDO_COMPLETADO',   contenido: 'The Office S02E03'   },
  { tipo: 'CALIFICACION_DADA',      contenido: 'Stranger Things', valor: 5 },
]

const publicarEventos = async () => {
  await inicializarNamespaces()
  const client   = crearCliente()
  const producer = await client.createProducer({ topic: TOPICS.EVENTOS_USUARIO })

  console.log('🎬 Producer STREAMING listo')
  console.log(`📡 Topic: ${TOPICS.EVENTOS_USUARIO}\n`)
  console.log('💡 Este topic usa modo STREAMING — múltiples consumers pueden')
  console.log('   leer los mismos eventos de forma independiente\n')

  // Publicar 10 eventos simulando actividad en tiempo real
  for (let i = 0; i < 10; i++) {
    const usuario = usuarios[Math.floor(Math.random() * usuarios.length)]
    const evento  = tiposEvento[Math.floor(Math.random() * tiposEvento.length)]

    const mensaje = {
      eventoId:  `evt-${Date.now()}-${i}`,
      usuarioId: usuario.id,
      nombre:    usuario.nombre,
      plan:      usuario.plan,
      tipo:      evento.tipo,
      contenido: evento.contenido,
      valor:     evento.valor || null,
      timestamp: new Date().toISOString(),
    }

    await producer.send({
      data: Buffer.from(JSON.stringify(mensaje)),
      // En Pulsar podés agregar propiedades al mensaje sin tocar el body
      properties: {
        tipo:   evento.tipo,
        userId: usuario.id,
      }
    })

    console.log(`📤 Evento [${i + 1}/10] → ${usuario.nombre}: ${evento.tipo}`)
    await new Promise(r => setTimeout(r, 300))
  }

  console.log('\n✅ 10 eventos publicados — analytics y recomendaciones los recibirán')

  await producer.close()
  await client.close()
}

publicarEventos().catch(console.error)