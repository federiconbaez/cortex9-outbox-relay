package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.application.port.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Reloj controlado a mano para tests deterministas. */
public final class ManualClock implements Clock {
  private Instant now;

  public ManualClock(Instant start) {
    this.now = Objects.requireNonNull(start);
  }

  @Override
  public synchronized Instant now() {
    return now;
  }

  public synchronized void advance(Duration duration) {
    if (duration.isNegative()) {
      throw new IllegalArgumentException("time does not go backwards");
    }
    now = now.plus(duration);
  }
}
