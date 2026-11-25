package io.github.federiconbaez.outbox.domain;

/** Identificador único de un mensaje de outbox; también es la clave de dedupe. */
public record MessageId(String value) {
  public MessageId {
    Text.requireNonBlank(value, "messageId");
  }

  @Override
  public String toString() {
    return value;
  }
}
