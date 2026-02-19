# mensajeria-distribuida — RabbitMQ Demo

Demo completo de RabbitMQ con Node.js simulando un e-commerce real con 4 servicios independientes.

##  Estructura

```
rabbitmq/
├── docker-compose.yml
├── config.js                          # configuración centralizada
├── producer.js                        # publica pedidos nuevos
└── consumers/
    ├── pedidos-consumer.js            # reserva stock
    ├── pagos-consumer.js              # procesa pagos
    ├── notificaciones-consumer.js     # envía email y push
    └── logs-consumer.js              # registra todo
```

##  Flujo de mensajes

```
[producer.js]
   "pedido nuevo"
        ↓
  Exchange FANOUT
  (ecommerce.pedidos)
   ↙    ↓      ↓      ↘
cola  cola   cola    cola
pedid pagos  notif   logs
  ↓     ↓     ↓       ↓
[consumer independiente por servicio]
```

Cada servicio recibe **el mismo mensaje** de forma independiente y simultánea.

## Cómo correrlo

### 1. Levantar RabbitMQ
```bash
docker-compose up -d
```

### 2. Instalar dependencias
```bash
npm install
```

### 3. Abrir 5 terminales

**Terminal 1:**
```bash
npm run logs
```

**Terminal 2:**
```bash
npm run notificaciones
```

**Terminal 3:**
```bash
npm run pagos
```

**Terminal 4:**
```bash
npm run pedidos
```

**Terminal 5 — publicar pedidos:**
```bash
npm run producer
```

### 4. Ver el panel web de RabbitMQ
Abrí el navegador en: http://localhost:15672
- Usuario: `admin`
- Contraseña: `admin123`

Podés ver las colas, mensajes pendientes, tasa de procesamiento y más.
