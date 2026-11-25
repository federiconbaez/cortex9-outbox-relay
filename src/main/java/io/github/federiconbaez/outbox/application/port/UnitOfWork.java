package io.github.federiconbaez.outbox.application.port;

import java.util.function.Function;

/**
 * Ejecuta {@code work} en una transacción. Si {@code work} lanza, no se confirma nada (ni el cambio
 * de negocio ni los mensajes).
 */
public interface UnitOfWork {
  <T> T inTransaction(Function<Transaction, T> work);
}
