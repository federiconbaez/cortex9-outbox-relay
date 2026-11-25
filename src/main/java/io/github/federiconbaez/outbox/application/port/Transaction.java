package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.OutboxMessage;

/** Vista de una transacción abierta: cambio de negocio y outbox se confirman juntos. */
public interface Transaction {
  void putRecord(String key, String value);

  void appendOutbox(OutboxMessage message);
}
