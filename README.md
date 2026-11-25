# cortex9-outbox-relay

Patrón **transactional outbox** en **Java 21**, arquitectura hexagonal, sin frameworks.
Proyecto de estudio y portfolio (CX9-6203): no hay broker ni base de datos reales; todo corre en
memoria con fakes escritos a mano.

## Qué hace

- **Escritura atómica** (`OutboxWriter` + puerto `UnitOfWork`): el cambio de negocio y los mensajes
  del outbox se confirman juntos. Si el trabajo lanza o el proceso "muere" antes del commit, no
  queda nada (ni registro de negocio ni mensaje huérfano).
- **Relay at-least-once** (`Relay`): libera leases vencidos, reclama un lote con lease, publica y
  marca el resultado. Si el lease venció antes de publicar, no publica.
- **Lease con fencing token** (ADR-0001): el token es el número de intento; un relay con lease viejo
  recibe `StaleLease` y no puede pisar el trabajo de otro.
- **Backoff exponencial con techo y jitter**: techo(n) = min(cap, base·2^(n-1)), demora en
  [techo/2, techo]. El azar entra por el puerto `RandomSource`; con semilla fija es determinista.
- **Cuarentena de mensajes venenosos**: tras `maxAttempts` fallos (transitorios o leases vencidos)
  o ante un `Permanent` del publisher.
- **Consumidor idempotente** (ADR-0002): `IdempotentConsumer` + `DedupeStore` convierten
  at-least-once en *effectively-once*: el efecto corre una vez por `MessageId`.
- **Estados y errores sellados**: `OutboxStatus` (`Pending | InFlight | Published | Quarantined`),
  `QuarantineReason`, `DomainError`, `Result`, `PublishResult` (`Ack | Transient | Permanent`),
  `UpdateOutcome`. Los `switch` son exhaustivos y los chequea el compilador.

## Qué NO hace

- No hay Kafka, RabbitMQ ni base real; tampoco CDC/log tailing ni HTTP.
- No garantiza orden por agregado después de reintentos.
- No limpia mensajes publicados ni pone TTL al `DedupeStore`.
- Asume un reloj común entre relays (en producción, el de la base). Con relojes desincronizados la
  exclusión por lease no está garantizada.
- La atomicidad "dedupe + efecto" del consumidor está simulada con un lock en memoria.

Detalle en [CONTRACT.md](CONTRACT.md) y [docs/adr](docs/adr).

## Estructura

```
domain/              OutboxMessage, OutboxStatus, Lease, BackoffPolicy, QuarantineReason,
                     DomainError, Result, MessageId, RelayId, Payload (solo JDK + jspecify)
application/         OutboxWriter, Relay, IdempotentConsumer, RelayConfig, RelayReport
  port/              OutboxStore, UnitOfWork, Transaction, Publisher, PublishResult, Envelope,
                     Clock, RandomSource, IdGenerator, DedupeStore, Transition, UpdateOutcome
adapter/inmemory/    InMemoryOutboxDatabase (UnitOfWork + OutboxStore, crashes inyectables),
                     InMemoryDedupeStore, ManualClock, SeededRandomSource, ...
adapter/fake/        ScriptedPublisher (ACK, TRANSIENT, PERMANENT, DELIVER_THEN_TRANSIENT, THROW)
```

## Calidad (medido en local con JDK Temurin 21.0.13 y en CI)

| Gate | Resultado |
|---|---|
| Tests (JUnit 5 + jqwik + ArchUnit) | **70 tests**, 0 fallas |
| Propiedades jqwik | 7 propiedades, 4.700 casos generados por corrida |
| Cobertura de ramas del dominio (JaCoCo) | **60/60 = 100 %** (el build falla si baja de 95 %) |
| Mutation testing PIT (dominio) | **79/80 = 98,8 %** (umbral 80 %) |
| Error Prone + NullAway | activos en código de producción |
| Formato | Spotless + google-java-format (check en `verify`) |
| Secretos | gitleaks en CI |
| Arquitectura | 7 reglas ArchUnit (dominio sin I/O, sin concurrencia, sin `java.util.Random`, puertos solo interfaces/records, sin frameworks) |

El mutante sobreviviente es equivalente: cambiar `value < cap` por `value <= cap` en la condición
del loop de `ceilingMillis` da el mismo techo.

### Propiedades que se verifican

1. **Terminación**: con fallos transitorios acotados (incluye "entregado pero el ack se perdió" y
   excepciones del cliente), todo mensaje confirmado termina `Published` o `Quarantined`, y en el
   estado exacto que predice su guion.
2. **Exclusión por lease**: tres relays intercalados al azar (reclamar, procesar, pausar, olvidar,
   crashear entre publicar y marcar). Cada publicación de un mensaje usa un token mayor y ocurre
   recién cuando el lease anterior terminó. Comprobado que la propiedad **falla** si se quita el
   chequeo de lease vigente antes de publicar.
3. **Crash entre publicar y marcar**: hay entregas duplicadas (en ~60 % de las corridas), pero cada
   mensaje produce **exactamente un** efecto en el consumidor.
4. **Backoff**: techo monótono y acotado por `cap`; demora dentro de [techo/2, techo]; con el mismo
   cuantil de jitter, demoras monótonas.

Sin Mockito: `ScriptedPublisher`, `InMemoryOutboxDatabase` con crashes inyectables, `ManualClock`,
`SeededRandomSource`. La suite abstracta `OutboxStoreContract` (8 casos, incluye 8 hilos
reclamando 500 mensajes sin duplicar ninguno) sirve para validar un futuro adapter JDBC.

## Cómo correrlo

```bash
mvn verify                                        # compila con Error Prone/NullAway, tests, JaCoCo, Spotless
mvn org.pitest:pitest-maven:mutationCoverage      # PIT sobre el dominio
```

Requiere JDK 21 y Maven 3.9.

## Cómo contarlo en una entrevista

- **El problema en una frase**: "escribir en la base y publicar en el broker no es atómico; el
  outbox mueve la publicación a la misma transacción y la delega a un relay con reintentos".
- **La decisión clave**: at-least-once en el relay + dedupe en el consumidor. Exactly-once
  extremo a extremo no existe sin un protocolo transaccional compartido; lo honesto es
  *effectively-once* por idempotencia, y tengo una propiedad que lo demuestra con crashes reales
  entre publicar y marcar.
- **Concurrencia entre relays**: lease con vencimiento + fencing token. Explicar el caso del relay
  pausado (GC) que despierta tarde: su marca se rechaza con `StaleLease`. Y el límite: depende de
  un reloj común.
- **Mensajes venenosos**: backoff exponencial con equal jitter para no sincronizar reintentos, y
  cuarentena con motivo para no bloquear la cola.
- **Cómo lo validé**: dominio puro con 100 % de ramas y 98,8 % de mutantes muertos, propiedades
  jqwik sobre el sistema completo, y una prueba de que la propiedad de exclusión detecta un bug
  plantado.
- **Qué haría en producción**: `SELECT ... FOR UPDATE SKIP LOCKED` para `claimDue`, reloj de la
  base, TTL en la tabla de dedupe, métricas de antigüedad del mensaje más viejo pendiente y
  alertas sobre la cuarentena.

## Licencia

Apache-2.0. Ver [LICENSE](LICENSE).
