package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.application.port.Clock;
import java.time.Instant;

public final class SystemClock implements Clock {
  @Override
  public Instant now() {
    return Instant.now();
  }
}
