package io.github.federiconbaez.outbox.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Reclamo temporal de un mensaje por un relay. {@code token} es el número de intento y funciona
 * como fencing token: solo quien tiene el token vigente puede marcar el mensaje.
 */
public record Lease(RelayId owner, int token, Instant expiresAt) {
  public Lease {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(expiresAt, "expiresAt");
    if (token < 1) {
      throw new IllegalArgumentException("token must be >= 1");
    }
  }

  public boolean isValidAt(Instant now) {
    return now.isBefore(expiresAt);
  }
}
