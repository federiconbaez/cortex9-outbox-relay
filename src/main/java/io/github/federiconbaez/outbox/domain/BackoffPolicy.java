package io.github.federiconbaez.outbox.domain;

import java.time.Duration;
import java.util.Objects;

/**
 * Backoff exponencial con techo y "equal jitter": techo(n) = min(cap, base * 2^(n-1)) y la demora
 * real es techo/2 + draw, con draw en [0, jitterBound(n)). El azar entra como parámetro para que el
 * dominio siga siendo determinista.
 */
public record BackoffPolicy(Duration base, Duration cap, int maxAttempts) {
  public BackoffPolicy {
    Objects.requireNonNull(base, "base");
    Objects.requireNonNull(cap, "cap");
    if (base.toMillis() < 1) {
      throw new IllegalArgumentException("base must be >= 1ms");
    }
    if (cap.compareTo(base) < 0) {
      throw new IllegalArgumentException("cap must be >= base");
    }
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1");
    }
  }

  /** Techo de la demora para el intento {@code attempt} (1-based), en milisegundos. */
  public long ceilingMillis(int attempt) {
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be >= 1");
    }
    long capMillis = cap.toMillis();
    long value = base.toMillis();
    for (int i = 1; i < attempt && value < capMillis; i++) {
      value = value > capMillis / 2 ? capMillis : value * 2;
    }
    return Math.min(value, capMillis);
  }

  /** Cota exclusiva para el sorteo de jitter: los valores válidos son [0, jitterBound). */
  public long jitterBound(int attempt) {
    long ceiling = ceilingMillis(attempt);
    return ceiling - ceiling / 2 + 1;
  }

  /** Demora para el intento {@code attempt} dado un sorteo en [0, jitterBound(attempt)). */
  public Duration delay(int attempt, long jitterDraw) {
    long bound = jitterBound(attempt);
    if (jitterDraw < 0 || jitterDraw >= bound) {
      throw new IllegalArgumentException("jitterDraw out of range [0," + bound + ")");
    }
    return Duration.ofMillis(ceilingMillis(attempt) / 2 + jitterDraw);
  }

  public boolean exhausted(int attempts) {
    return attempts >= maxAttempts;
  }
}
