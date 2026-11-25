# ADR-0001: Lease con vencimiento y fencing token por intento

Estado: aceptada

## Contexto
Varios relays pueden correr a la vez. Sin coordinación, dos relays publican el mismo mensaje en
paralelo, y un relay lento (pausa de GC, red) puede marcar como publicado algo que otro ya reintentó.

## Decisión
`claim` mueve el mensaje a `InFlight(Lease(owner, token, expiresAt))`, con `token = attempts`
después de incrementar. `OutboxStore.claimDue` es atómico. Toda marca (`markPublished`,
`markTransientFailure`, `quarantine`) exige que el lease actual tenga el mismo `owner` y `token`;
si no, devuelve `StaleLease`. Solo `releaseExpiredLease` (con `now >= expiresAt`) devuelve el mensaje
a `Pending`, y cuenta como intento fallido. El relay no publica si su lease ya venció.

## Consecuencias
- Dentro de un lease vigente publica un solo relay (lo prueba una propiedad jqwik).
- Un relay que se cae deja el mensaje `InFlight` hasta que vence el lease: la latencia de
  recuperación es la duración del lease.
- Un relay que publicó pero no llegó a marcar provoca una **reentrega**: es at-least-once, no
  exactly-once. Lo resuelve el consumidor (ADR-0002).
- Los crashes repetidos consumen intentos, así que un mensaje que tumba al relay termina en cuarentena.
