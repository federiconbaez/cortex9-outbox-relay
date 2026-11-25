package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.domain.BackoffPolicy;
import java.time.Duration;
import java.util.Objects;

public record RelayConfig(int batchSize, Duration leaseDuration, BackoffPolicy backoff) {
  public RelayConfig {
    Objects.requireNonNull(leaseDuration, "leaseDuration");
    Objects.requireNonNull(backoff, "backoff");
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be >= 1");
    }
    if (leaseDuration.isNegative() || leaseDuration.isZero()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
  }
}
