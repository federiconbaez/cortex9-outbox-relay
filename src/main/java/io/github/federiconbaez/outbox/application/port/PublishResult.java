package io.github.federiconbaez.outbox.application.port;

/** Respuesta del broker. */
public sealed interface PublishResult {
  record Ack() implements PublishResult {}

  /** Puede funcionar más tarde (timeout, broker caído). */
  record Transient(String reason) implements PublishResult {}

  /** No va a funcionar nunca (mensaje inválido, topic inexistente): va a cuarentena. */
  record Permanent(String reason) implements PublishResult {}
}
