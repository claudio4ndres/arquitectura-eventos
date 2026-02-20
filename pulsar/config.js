const Pulsar = require('pulsar-client')

const PULSAR_URL = 'pulsar://localhost:6650'

// En Pulsar los topics tienen una estructura jerárquica:
// persistent://tenant/namespace/topic
// - persistent = los mensajes se guardan en disco (también existe non-persistent)
// - tenant     = la "empresa" o equipo dueño del topic
// - namespace  = agrupación lógica (como un "proyecto" o "dominio")
// - topic      = el canal de mensajes

const TOPICS = {
  // Modo STREAMING — eventos de comportamiento del usuario (como Kafka)
  EVENTOS_USUARIO:   'persistent://plataforma/streaming/eventos-usuario',

  // Modo QUEUE — tareas que deben procesarse una sola vez (como RabbitMQ)
  NOTIFICACIONES:    'persistent://plataforma/queue/notificaciones',
}

const crearCliente = () => new Pulsar.Client({ serviceUrl: PULSAR_URL })

// Pulsar requiere que el tenant y los namespaces existan antes de publicar.
// Esta función los crea via Admin API si no existen.
const inicializarNamespaces = async () => {
  const base = 'http://localhost:8080/admin/v2'

  const crearSiNoExiste = async (url, body = {}) => {
    const res = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    // 204 = creado, 409 = ya existe, ambos están bien
    if (res.status !== 204 && res.status !== 409) {
      const text = await res.text()
      throw new Error(`Error ${res.status}: ${text}`)
    }
  }

  console.log('🔧 Inicializando tenant y namespaces en Pulsar...')

  // Crear tenant "plataforma"
  await crearSiNoExiste(`${base}/tenants/plataforma`, {
    adminRoles: [],
    allowedClusters: ['standalone'],
  })

  // Crear namespaces
  await crearSiNoExiste(`${base}/namespaces/plataforma/streaming`)
  await crearSiNoExiste(`${base}/namespaces/plataforma/queue`)

  console.log('✅ Namespaces listos: plataforma/streaming y plataforma/queue\n')
}

module.exports = { PULSAR_URL, TOPICS, crearCliente, inicializarNamespaces }