package io.github.federiconbaez.outbox.domain;

import static io.github.federiconbaez.outbox.domain.Fixtures.A;
import static io.github.federiconbaez.outbox.domain.Fixtures.LEASE;
import static io.github.federiconbaez.outbox.domain.Fixtures.T0;
import static io.github.federiconbaez.outbox.domain.Fixtures.lease;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

class OutboxMessagePropertyTest {

  /** Con fallos transitorios, el número de intentos nunca supera maxAttempts y crece de a uno. */
  @Property
  void attemptsAreBoundedByPolicy(
      @ForAll @IntRange(min = 1, max = 8) int maxAttempts,
      @ForAll @Size(max = 20) List<Boolean> transientOutcomes) {
    BackoffPolicy policy =
        new BackoffPolicy(Duration.ofMillis(10), Duration.ofSeconds(1), maxAttempts);
    OutboxMessage m = Fixtures.pending("m");
    Instant now = T0;
    for (boolean fail : transientOutcomes) {
      if (m.isTerminal()) {
        break;
      }
      now = m.nextAttemptAt().isAfter(now) ? m.nextAttemptAt() : now;
      OutboxMessage claimed = Fixtures.ok(m.claim(A, now, LEASE));
      assertThat(claimed.attempts()).isEqualTo(m.attempts() + 1);
      m =
          fail
              ? Fixtures.ok(
                  claimed.markTransientFailure(lease(claimed), now, "t", policy, b -> b / 2))
              : Fixtures.ok(claimed.markPublished(lease(claimed), now));
      assertThat(m.attempts()).isLessThanOrEqualTo(maxAttempts);
      if (m.status() instanceof OutboxStatus.Pending) {
        assertThat(m.nextAttemptAt()).isAfter(now);
      }
    }
  }
}
