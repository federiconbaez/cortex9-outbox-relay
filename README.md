# cortex9-outbox-relay

Patrón **transactional outbox** en **Java 21**, arquitectura hexagonal, sin frameworks.
Proyecto de estudio y portfolio (CX9-6203, CX9-6204). No hay broker real. El outbox y el dedupe
tienen dos implementaciones: fakes en memoria escritos a mano y un adapter durable sobre **SQLite**.
Las dos pasan la misma suite de contrato.

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
- **Adapter durable SQLite** (ADR-0003): `SqliteOutboxDatabase` y `SqliteDedupeStore` usan
  transacciones `BEGIN IMMEDIATE`, WAL y esquema versionado. Corren las mismas suites de contrato que
  los fakes. También tienen pruebas de reinicio (cerrar y reabrir), de dos conexiones sobre el mismo
  archivo y de ida y vuelta sin pérdida (propiedad jqwik).
- **Estados y errores sellados**: `OutboxStatus` (`Pending | InFlight | Published | Quarantined`),
  `QuarantineReason`, `DomainError`, `Result`, `PublishResult` (`Ack | Transient | Permanent`),
  `UpdateOutcome`. Los `switch` son exhaustivos y los chequea el compilador.

## Qué NO hace

- No hay Kafka, RabbitMQ, CDC/log tailing ni HTTP. No hay adapter para Postgres/MySQL.
- SQLite serializa a todos los escritores: es un adapter durable para un nodo, o para pocos procesos
  en la misma máquina. No sirve para varios hosts ni para un sistema de archivos de red.
- No garantiza orden por agregado después de reintentos.
- No limpia mensajes publicados ni pone TTL al `DedupeStore`.
- Asume un reloj común entre relays (en producción, el de la base). Con relojes desincronizados la
  exclusión por lease no está garantizada.
- La atomicidad "dedupe + efecto" del consumidor solo es real si el efecto escribe en la misma
  base. Con `SqliteDedupeStore`, un efecto externo puede repetirse si el proceso muere entre el
  efecto y el commit. En memoria se simula con un lock.

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
adapter/sqlite/      SqliteOutboxDatabase, SqliteDedupeStore, OutboxRows (fila <-> dominio),
                     StorageException (único paquete que ve java.sql)
adapter/fake/        ScriptedPublisher (ACK, TRANSIENT, PERMANENT, DELIVER_THEN_TRANSIENT, THROW)
```

## Calidad (medido en local con JDK Temurin 21.0.13 y en CI)

| Gate | Resultado |
|---|---|
| Tests (JUnit 5 + jqwik + ArchUnit) | **103 tests**, 0 fallas |
| Propiedades jqwik | 8 propiedades, 5.000 casos generados por corrida (300 de ida y vuelta por SQLite) |
| Cobertura de ramas del dominio (JaCoCo) | **60/60 = 100 %** (el build falla si baja de 95 %) |
| Cobertura de ramas del adapter SQLite (JaCoCo) | 58/64 = 90,6 % (informativo) |
| Mutation testing PIT (dominio) | **79/80 = 98,8 %** (umbral 80 %) |
| Mutation testing PIT (adapter SQLite) | **106/114 = 93,0 %** (umbral 80 % en CI) |
| Error Prone + NullAway | activos en código de producción |
| Formato | Spotless + google-java-format (check en `verify`) |
| Secretos | gitleaks en CI |
| Arquitectura | 9 reglas ArchUnit (dominio sin I/O, sin concurrencia, sin `java.util.Random`, puertos solo interfaces/records, sin frameworks, `java.sql` solo en `adapter.sqlite`, adapters sin dependencias cruzadas) |

El mutante sobreviviente del dominio es equivalente: cambiar `value < cap` por `value <= cap` en
la condición del loop de `ceilingMillis` da el mismo techo.

Los 8 mutantes que sobreviven en el adapter SQLite son equivalentes o defensivos. Los revisé uno
por uno:
- `setNull` sobre un parámetro que, si no se liga, ya vale NULL.
- Devolver `Boolean.FALSE` en vez de `TRUE` en lambdas cuyo valor nadie usa.
- El chequeo "la fila desapareció durante el update", que es inalcanzable bajo el lock de escritura.
- El rollback tras un `COMMIT` fallido, que no sé provocar de forma determinista.

Un mutante manual también sobrevive: sacar la comparación de nanosegundos del `WHERE` de
`claimDue`. Es redundante, porque el orden por (segundos, nanos) y el chequeo `isDue` del dominio
ya excluyen lo que no venció. Sí lo detectan otros dos mutantes manuales: cambiar `BEGIN IMMEDIATE`
por `BEGIN` (lo detecta la prueba de dos conexiones) y perder los nanos al escribir (lo detecta la
propiedad de ida y vuelta).

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
`SeededRandomSource`. Las suites abstractas `OutboxStoreContract` (12 casos, incluye 8 hilos
reclamando 500 mensajes sin duplicar ninguno y la precisión de nanosegundos en `nextAttemptAt`) y
`DedupeStoreContract` (3 casos, incluye 8 entregas concurrentes del mismo id) corren contra los
fakes y contra SQLite. `SqliteDurabilityTest` agrega lo que solo una base real puede probar:
- el estado sobrevive a cerrar y reabrir;
- dos conexiones con dos hilos cada una no reclaman nada dos veces;
- un relay que se reinicia retoma el trabajo y el consumidor aplica cada efecto una vez;
- una fila corrupta o un error de SQL da `StorageException` con rollback;
- un esquema más nuevo hace que el adapter se niegue a abrir.

## Cómo correrlo

```bash
mvn verify                                        # compila con Error Prone/NullAway, tests, JaCoCo, Spotless
mvn org.pitest:pitest-maven:mutationCoverage      # PIT sobre el dominio
mvn org.pitest:pitest-maven:mutationCoverage \
  -DtargetClasses='io.github.federiconbaez.outbox.adapter.sqlite.*' \
  -DexcludedTestClasses='*ArchitectureTest'       # PIT sobre el adapter SQLite
```

Uso del adapter durable:

```java
try (var db = SqliteOutboxDatabase.open(Path.of("outbox.db"))) {
  var writer = new OutboxWriter(db, clock, ids);
  var relay = new Relay(new RelayId("r1"), db, publisher, clock, random, config);
  relay.runOnce();
}
```

Requiere JDK 21 y Maven 3.9. `sqlite-jdbc` trae la biblioteca nativa para Linux, macOS y Windows.

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
- **Qué haría en producción**: un adapter Postgres que pase la misma suite de contrato, con
  `SELECT ... FOR UPDATE SKIP LOCKED` para `claimDue`. SQLite ya demuestra que el contrato se
  sostiene con I/O real, pero serializa a los escritores. Además: reloj de la base, TTL en la tabla de dedupe, métricas de antigüedad del mensaje más viejo pendiente y
  alertas sobre la cuarentena.

## Licencia

Apache-2.0. Ver [LICENSE](LICENSE).
