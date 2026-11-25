# ADR-0002: At-least-once en el relay + dedupe en el consumidor

Estado: aceptada

## Contexto
Entre "el broker aceptó el mensaje" y "el outbox lo marcó publicado" siempre hay una ventana. Un
crash ahí obliga a elegir: perder el mensaje (at-most-once) o reenviarlo (at-least-once).

## Decisión
El relay elige at-least-once. El consumidor usa `IdempotentConsumer`, que delega en
`DedupeStore.runOnce(messageId, efecto)`: el id solo queda registrado si el efecto terminó, y un id
ya registrado no vuelve a ejecutar el efecto.

## Consecuencias
- Effectively-once = at-least-once + dedupe por id. Lo prueba una propiedad con crashes entre
  publicar y marcar: hay entregas duplicadas, pero cada mensaje produce exactamente un efecto.
- En producción, el registro de dedupe y el efecto tienen que vivir en la misma transacción del
  consumidor. Acá el adapter en memoria lo simula con un lock; no es una garantía distribuida.
- El `DedupeStore` crece sin límite: en producción necesita TTL mayor que la ventana de reentrega.
