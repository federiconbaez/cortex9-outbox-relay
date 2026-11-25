package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.MessageId;

/** Registro de mensajes ya procesados del lado consumidor. */
public interface DedupeStore {
  /**
   * Ejecuta {@code effect} si {@code id} no fue procesado y lo registra solo si el efecto terminó
   * sin lanzar. Devuelve true si ejecutó el efecto. Debe ser atómico por id.
   */
  boolean runOnce(MessageId id, Runnable effect);

  boolean seen(MessageId id);
}
