# ADR-0003: Adapter durable sobre SQLite con la misma suite de contrato

Estado: aceptada (CX9-6204)

## Contexto
Hasta ahora todo vivía en memoria. La suite `OutboxStoreContract` existía "para un futuro adapter
JDBC", pero nunca había corrido contra una base real, así que no probaba que las garantías
(atomicidad, reclamo sin duplicados, fencing) sobrevivieran a I/O real, reinicios y varias
conexiones.

## Decisión
- Nuevo paquete `adapter.sqlite`: `SqliteOutboxDatabase` (`UnitOfWork` + `OutboxStore`) y
  `SqliteDedupeStore` (`DedupeStore`), con `sqlite-jdbc` como dependencia de runtime.
- Cada operación es una transacción `BEGIN IMMEDIATE`: toma el lock de escritura del archivo al
  empezar. Dentro del proceso, además, un lock serializa el uso de la conexión. Entre procesos,
  `busy_timeout` hace que esperen en lugar de fallar. WAL + `synchronous=FULL`.
- Las reglas siguen en el dominio: el adapter lee la fila, llama a la transición pura
  (`claim`, `markPublished`, ...) y escribe el resultado en la misma transacción. El SQL solo
  preselecciona candidatos.
- La suite de contrato pasa a `adapter.contract` (tests) y se ejecuta sin cambios contra el fake en
  memoria y contra SQLite. Se agrega `DedupeStoreContract`, que también corre contra ambos.
- `java.sql` y `org.sqlite` solo pueden aparecer en `adapter.sqlite` (regla ArchUnit).
- Las fallas de infraestructura son `StorageException` (unchecked). Los errores de negocio siguen
  siendo `DomainError`.
- Esquema versionado con `PRAGMA user_version` (versión 1). Si la versión en disco es más nueva, el
  adapter se niega a abrir.

## Consecuencias
- Se prueba lo que el fake no puede probar: el estado sobrevive a cerrar y reabrir, dos conexiones
  independientes al mismo archivo no reclaman el mismo mensaje, y un relay que se reinicia retoma
  el trabajo sin que el consumidor duplique efectos.
- SQLite serializa a todos los escritores: no hay `SKIP LOCKED` ni concurrencia real de escritura.
  Sirve como adapter durable de un nodo o de pocos procesos en la misma máquina. No sirve para
  varios hosts, porque el lock de SQLite sobre sistemas de archivos de red no es confiable.
- `SqliteDedupeStore` registra el id en la misma transacción en la que corre el efecto, pero el
  efecto no forma parte de esa transacción. Si el efecto es externo y el proceso muere después de
  ejecutarlo y antes del commit, el efecto se repite (ver ADR-0002).
