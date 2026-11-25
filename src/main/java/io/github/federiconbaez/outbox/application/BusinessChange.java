package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.application.port.Transaction;

/** Cambio de negocio que se confirma junto con sus mensajes. */
@FunctionalInterface
public interface BusinessChange {
  void apply(Transaction tx);
}
