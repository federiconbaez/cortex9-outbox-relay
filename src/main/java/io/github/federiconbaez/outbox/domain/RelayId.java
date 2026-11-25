package io.github.federiconbaez.outbox.domain;

/** Identidad de una instancia de relay (dueño de un lease). */
public record RelayId(String value) {
  public RelayId {
    Text.requireNonBlank(value, "relayId");
  }

  @Override
  public String toString() {
    return value;
  }
}
