# CONTRACT — cortex9-outbox-relay (CX9-6203)

Patrón **transactional outbox** en Java 21, hexagonal, sin frameworks, sin broker ni base reales.
Este contrato se escribió antes del código y define qué se promete y qué no.

## Problema
Un servicio cambia su estado y tiene que avisar a otros (publicar un evento). Si escribe en la base
y después publica, un crash entre ambos pasos pierde el evento; si publica primero, puede anunciar
algo que nunca se confirmó. El outbox guarda el mensaje **en la misma transacción** que el cambio de
negocio y un **relay** lo publica después, con reintentos.

## Garantías
1. **Atomicidad de escritura**: el cambio de negocio y sus mensajes de outbox se confirman juntos o
   no se confirma ninguno (`UnitOfWork`). Un crash antes del commit no deja mensajes huérfanos.
2. **At-least-once**: todo mensaje confirmado termina `Published` o `Quarantined`, siempre que los
   fallos transitorios sean acotados y el relay siga corriendo. Puede entregarse más de una vez.
3. **Exclusión por lease**: un relay solo publica un mensaje que tiene reclamado (`InFlight`) con
   lease vigente según el `Clock`. Otro relay solo puede reclamarlo después de que el lease venció y
   se liberó. Las marcas usan un **fencing token** (número de intento): un relay con lease viejo no
   puede marcar nada.
4. **Effectively-once en el consumidor**: `IdempotentConsumer` + `DedupeStore` aplican el efecto
   una sola vez por `MessageId`, aunque el mensaje llegue duplicado.
5. **Backoff exponencial acotado**: techo(n) = min(cap, base · 2^(n-1)); la demora real está en
   [techo/2, techo] (equal jitter). El techo es monótono no decreciente y nunca supera `cap`.
   El jitter sale del puerto `RandomSource`; con semilla fija el resultado es determinista.
6. **Cuarentena de mensajes venenosos**: tras `maxAttempts` intentos fallidos (transitorios o
   leases vencidos) o ante un error permanente del publisher, el mensaje pasa a `Quarantined` con
   su motivo y no se reintenta más.

## Modelo de dominio (sin I/O, solo JDK + jspecify)
- `OutboxMessage(id, aggregateId, type, payload, attempts, status, nextAttemptAt, createdAt)`.
- `OutboxStatus` sellado: `Pending` | `InFlight(Lease)` | `Published(at, by)` |
  `Quarantined(QuarantineReason, at)`.
- `Lease(owner, token, expiresAt)`; `token` = número de intento al reclamar.
- `QuarantineReason` sellado: `MaxAttemptsExceeded(attempts, lastError)` | `PermanentFailure(reason)`.
- `DomainError` sellado: `NotDue`, `NotClaimable`, `StaleLease`, `LeaseStillValid`.
- `Result<T>` sellado: `Ok` | `Err(DomainError)`. Las reglas de negocio no lanzan excepciones;
  las excepciones quedan para errores de programación (nulls, valores fuera de rango).
- `BackoffPolicy(base, cap, maxAttempts)`.

### Transiciones
| Desde | Operación | Hacia |
|---|---|---|
| Pending (due) | `claim(owner, now, lease)` | InFlight, `attempts+1` |
| InFlight (token ok) | `markPublished` | Published |
| InFlight (token ok) | `markTransientFailure` | Pending con backoff, o Quarantined si `attempts >= max` |
| InFlight (token ok) | `quarantine` (error permanente) | Quarantined |
| InFlight (lease vencido) | `releaseExpiredLease` | Pending con backoff, o Quarantined si `attempts >= max` |
| Published / Quarantined | cualquiera | error tipado (terminales) |

## Puertos (application.port)
- `UnitOfWork` / `Transaction`: cambio de negocio + `appendOutbox` atómicos.
- `OutboxStore`: `claimDue` (atómico, con lease), `update(id, Transition)` (read-modify-write
  atómico), `expiredLeases`, `find`, `snapshot`.
- `Publisher` → `PublishResult` sellado: `Ack` | `Transient(reason)` | `Permanent(reason)`.
- `Clock`, `RandomSource`, `IdGenerator`, `DedupeStore` (lado consumidor).

## Aplicación
- `OutboxWriter`: aplica el cambio de negocio y agrega los mensajes en una transacción.
- `Relay`: libera leases vencidos, reclama un lote, publica, marca. Si el lease venció antes de
  publicar, no publica. Una excepción del publisher se trata como fallo transitorio.
- `IdempotentConsumer`: dedupe por `MessageId`.

## Adapters
- `adapter.inmemory`: `InMemoryOutboxDatabase` (UnitOfWork + OutboxStore, con inyección de crash
  antes del commit y antes de la próxima marca), `InMemoryDedupeStore`, `ManualClock`,
  `SeededRandomSource`, `SequentialIdGenerator`, `SystemClock`.
- `adapter.fake`: `ScriptedPublisher` con fallos guionados por mensaje (incluye "entregado pero el
  ack se perdió").

## Fuera de alcance
- Broker real (Kafka, RabbitMQ), base real, CDC/log tailing, orden global entre agregados,
  métricas, persistencia del `DedupeStore`, limpieza de mensajes publicados.
- El orden de entrega por agregado **no** se garantiza tras reintentos.
- Relojes desincronizados entre relays: se asume un `Clock` común (en producción, el de la base).

## Calidad exigida
JUnit 5 con fakes escritos a mano (sin Mockito), propiedades jqwik, suite de contrato de
`OutboxStore`, ArchUnit, JaCoCo de ramas en dominio, PIT en dominio >= 80 %, Error Prone + NullAway,
Spotless, gitleaks, CI verde en `main`. Licencia Apache-2.0.
