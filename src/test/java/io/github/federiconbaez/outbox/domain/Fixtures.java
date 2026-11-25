package io.github.federiconbaez.outbox.domain;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

final class Fixtures {
  static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  static final RelayId A = new RelayId("relay-a");
  static final RelayId B = new RelayId("relay-b");
  static final Duration LEASE = Duration.ofSeconds(30);
  static final BackoffPolicy POLICY =
      new BackoffPolicy(Duration.ofMillis(100), Duration.ofSeconds(10), 3);

  private Fixtures() {}

  static OutboxMessage pending(String id) {
    return OutboxMessage.pending(
        new MessageId(id),
        "order-1",
        "OrderPlaced",
        Payload.of("{}".getBytes(StandardCharsets.UTF_8)),
        T0);
  }

  static <T> T ok(Result<T> result) {
    if (result instanceof Result.Ok<T> ok) {
      return ok.value();
    }
    throw new AssertionError("expected Ok but was " + result);
  }

  static DomainError err(Result<?> result) {
    if (result instanceof Result.Err<?> err) {
      return err.error();
    }
    throw new AssertionError("expected Err but was " + result);
  }

  static Lease lease(OutboxMessage m) {
    return ((OutboxStatus.InFlight) m.status()).lease();
  }
}
