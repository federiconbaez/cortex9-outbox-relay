package io.github.federiconbaez.outbox.application;

/** Qué pasó con un mensaje reclamado. */
public enum ProcessOutcome {
  PUBLISHED,
  RETRY_SCHEDULED,
  QUARANTINED,
  /** Otro relay tomó el mensaje o el lease se liberó antes de marcar (fencing). */
  LEASE_LOST,
  /** El lease venció antes de publicar: no se publicó para no competir con otro relay. */
  LEASE_EXPIRED_BEFORE_PUBLISH
}
