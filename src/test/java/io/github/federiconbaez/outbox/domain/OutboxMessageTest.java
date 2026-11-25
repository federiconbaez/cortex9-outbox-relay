package io.github.federiconbaez.outbox.domain;

import static io.github.federiconbaez.outbox.domain.Fixtures.A;
import static io.github.federiconbaez.outbox.domain.Fixtures.B;
import static io.github.federiconbaez.outbox.domain.Fixtures.LEASE;
import static io.github.federiconbaez.outbox.domain.Fixtures.POLICY;
import static io.github.federiconbaez.outbox.domain.Fixtures.T0;
import static io.github.federiconbaez.outbox.domain.Fixtures.err;
import static io.github.federiconbaez.outbox.domain.Fixtures.lease;
import static io.github.federiconbaez.outbox.domain.Fixtures.ok;
import static io.github.federiconbaez.outbox.domain.Fixtures.pending;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.domain.QuarantineReason.MaxAttemptsExceeded;
import io.github.federiconbaez.outbox.domain.QuarantineReason.PermanentFailure;
import java.time.Duration;
import java.time.Instant;
import java.util.function.LongUnaryOperator;
import org.junit.jupiter.api.Test;

class OutboxMessageTest {
  private static final LongUnaryOperator NO_JITTER = bound -> 0L;
  private static final LongUnaryOperator MAX_JITTER = bound -> bound - 1;

  @Test
  void newMessageIsPendingAndDueImmediately() {
    OutboxMessage m = pending("m-1");
    assertThat(m.status()).isEqualTo(new OutboxStatus.Pending());
    assertThat(m.attempts()).isZero();
    assertThat(m.nextAttemptAt()).isEqualTo(T0);
    assertThat(m.createdAt()).isEqualTo(T0);
    assertThat(m.isDue(T0)).isTrue();
    assertThat(m.isDue(T0.minusMillis(1))).isFalse();
    assertThat(m.isTerminal()).isFalse();
  }

  @Test
  void claimMovesToInFlightWithLeaseAndCountsAttempt() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    assertThat(claimed.attempts()).isEqualTo(1);
    assertThat(lease(claimed)).isEqualTo(new Lease(A, 1, T0.plus(LEASE)));
    assertThat(claimed.isDue(T0)).isFalse();
    assertThat(claimed.id()).isEqualTo(new MessageId("m-1"));
    assertThat(claimed.payload()).isEqualTo(pending("m-1").payload());
  }

  @Test
  void cannotClaimTwiceOrBeforeDue() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    assertThat(err(claimed.claim(B, T0, LEASE)))
        .isEqualTo(new DomainError.NotClaimable(claimed.id(), claimed.status()));
    OutboxMessage retry =
        ok(claimed.markTransientFailure(lease(claimed), T0, "x", POLICY, NO_JITTER));
    assertThat(err(retry.claim(A, T0, LEASE)))
        .isEqualTo(new DomainError.NotDue(retry.id(), retry.nextAttemptAt()));
    assertThat(retry.claim(A, retry.nextAttemptAt(), LEASE)).isInstanceOf(Result.Ok.class);
  }

  @Test
  void claimRejectsNonPositiveLease() {
    assertThatThrownBy(() -> pending("m").claim(A, T0, Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> pending("m").claim(A, T0, Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void ownerWithCurrentLeaseCanMarkPublished() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    Instant at = T0.plusSeconds(1);
    OutboxMessage published = ok(claimed.markPublished(lease(claimed), at));
    assertThat(published.status()).isEqualTo(new OutboxStatus.Published(at, A));
    assertThat(published.isTerminal()).isTrue();
    assertThat(published.attempts()).isEqualTo(1);
  }

  @Test
  void ackAfterLeaseExpiryIsStillAcceptedIfNobodyReclaimed() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    assertThat(claimed.markPublished(lease(claimed), T0.plus(LEASE).plusSeconds(5)))
        .isInstanceOf(Result.Ok.class);
  }

  @Test
  void staleOwnerOrTokenCannotMark() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    Lease foreign = new Lease(B, 1, T0.plus(LEASE));
    Lease oldToken = new Lease(A, 2, T0.plus(LEASE));
    assertThat(err(claimed.markPublished(foreign, T0)))
        .isEqualTo(new DomainError.StaleLease(claimed.id(), B, 1));
    assertThat(err(claimed.markPublished(oldToken, T0)))
        .isEqualTo(new DomainError.StaleLease(claimed.id(), A, 2));
    assertThat(err(claimed.markTransientFailure(foreign, T0, "x", POLICY, NO_JITTER)))
        .isInstanceOf(DomainError.StaleLease.class);
    assertThat(err(claimed.quarantine(foreign, T0, "bad")))
        .isInstanceOf(DomainError.StaleLease.class);
    assertThat(err(pending("m-2").markPublished(foreign, T0)))
        .isInstanceOf(DomainError.StaleLease.class);
  }

  @Test
  void reclaimedMessageRejectsTheOldLease() {
    OutboxMessage claimedByA = ok(pending("m-1").claim(A, T0, LEASE));
    Instant expired = T0.plus(LEASE);
    OutboxMessage released = ok(claimedByA.releaseExpiredLease(expired, POLICY, NO_JITTER));
    OutboxMessage claimedByB = ok(released.claim(B, released.nextAttemptAt(), LEASE));
    assertThat(err(claimedByB.markPublished(lease(claimedByA), expired)))
        .isEqualTo(new DomainError.StaleLease(claimedByB.id(), A, 1));
    assertThat(claimedByB.markPublished(lease(claimedByB), expired)).isInstanceOf(Result.Ok.class);
  }

  @Test
  void transientFailureSchedulesRetryWithBackoff() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    Instant now = T0.plusSeconds(1);
    OutboxMessage low =
        ok(claimed.markTransientFailure(lease(claimed), now, "t", POLICY, NO_JITTER));
    OutboxMessage high =
        ok(claimed.markTransientFailure(lease(claimed), now, "t", POLICY, MAX_JITTER));
    assertThat(low.status()).isEqualTo(new OutboxStatus.Pending());
    assertThat(low.attempts()).isEqualTo(1);
    assertThat(low.nextAttemptAt()).isEqualTo(now.plusMillis(50));
    assertThat(high.nextAttemptAt()).isEqualTo(now.plusMillis(100));
  }

  @Test
  void transientFailureAtMaxAttemptsQuarantines() {
    OutboxMessage m = pending("m-1");
    Instant now = T0;
    for (int i = 1; i < POLICY.maxAttempts(); i++) {
      m = ok(m.claim(A, now, LEASE));
      m = ok(m.markTransientFailure(lease(m), now, "t" + i, POLICY, NO_JITTER));
      assertThat(m.status()).isInstanceOf(OutboxStatus.Pending.class);
      now = m.nextAttemptAt();
    }
    m = ok(m.claim(A, now, LEASE));
    OutboxMessage last = ok(m.markTransientFailure(lease(m), now, "boom", POLICY, NO_JITTER));
    assertThat(last.status())
        .isEqualTo(new OutboxStatus.Quarantined(new MaxAttemptsExceeded(3, "boom"), now));
    assertThat(last.nextAttemptAt()).isEqualTo(m.nextAttemptAt());
    assertThat(last.isTerminal()).isTrue();
  }

  @Test
  void permanentFailureQuarantinesImmediately() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    OutboxMessage q = ok(claimed.quarantine(lease(claimed), T0, "schema"));
    assertThat(q.status())
        .isEqualTo(new OutboxStatus.Quarantined(new PermanentFailure("schema"), T0));
    assertThat(q.attempts()).isEqualTo(1);
    assertThat(err(q.claim(A, T0, LEASE))).isInstanceOf(DomainError.NotClaimable.class);
  }

  @Test
  void releaseOnlyAfterLeaseExpiry() {
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    Instant justBefore = T0.plus(LEASE).minusMillis(1);
    assertThat(err(claimed.releaseExpiredLease(justBefore, POLICY, NO_JITTER)))
        .isEqualTo(new DomainError.LeaseStillValid(claimed.id(), T0.plus(LEASE)));
    Instant at = T0.plus(LEASE);
    OutboxMessage released = ok(claimed.releaseExpiredLease(at, POLICY, NO_JITTER));
    assertThat(released.status()).isEqualTo(new OutboxStatus.Pending());
    assertThat(released.attempts()).isEqualTo(1);
    assertThat(released.nextAttemptAt()).isEqualTo(at.plusMillis(50));
    assertThat(err(pending("m-2").releaseExpiredLease(at, POLICY, NO_JITTER)))
        .isInstanceOf(DomainError.NotClaimable.class);
  }

  @Test
  void repeatedLeaseExpiryEventuallyQuarantines() {
    BackoffPolicy one = new BackoffPolicy(Duration.ofMillis(1), Duration.ofMillis(1), 1);
    OutboxMessage claimed = ok(pending("m-1").claim(A, T0, LEASE));
    OutboxMessage q = ok(claimed.releaseExpiredLease(T0.plus(LEASE), one, NO_JITTER));
    assertThat(q.status())
        .isEqualTo(
            new OutboxStatus.Quarantined(
                new MaxAttemptsExceeded(1, "lease expired"), T0.plus(LEASE)));
  }

  @Test
  void resultAndReasonsRejectNulls() {
    assertThatThrownBy(() -> Result.ok(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Result.err(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new PermanentFailure(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new MaxAttemptsExceeded(1, null))
        .isInstanceOf(NullPointerException.class);
    assertThat(new DomainError.NotDue(new MessageId("x"), T0).messageId())
        .isEqualTo(new MessageId("x"));
  }
}
