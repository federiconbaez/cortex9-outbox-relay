package io.github.federiconbaez.outbox.adapter.fake;

/** Paso guionado de {@link ScriptedPublisher}. */
public enum Step {
  /** Entrega al consumidor y confirma. */
  ACK,
  /** No entrega; fallo transitorio. */
  TRANSIENT,
  /** No entrega; error permanente. */
  PERMANENT,
  /** Entrega al consumidor pero el ack se pierde: el relay ve un fallo transitorio. */
  DELIVER_THEN_TRANSIENT,
  /** El cliente del broker lanza una excepción sin entregar. */
  THROW
}
