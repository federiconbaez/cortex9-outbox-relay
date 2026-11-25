package io.github.federiconbaez.outbox.domain;

import io.github.federiconbaez.outbox.domain.OutboxStatus.InFlight;
import io.github.federiconbaez.outbox.domain.OutboxStatus.Pending;
import io.github.federiconbaez.outbox.domain.OutboxStatus.Published;
import io.github.federiconbaez.outbox.domain.OutboxStatus.Quarantined;
import io.github.federiconbaez.outbox.domain.QuarantineReason.MaxAttemptsExceeded;
import io.github.federiconbaez.outbox.domain.QuarantineReason.PermanentFailure;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.LongUnaryOperator;
import org.jspecify.annotations.Nullable;

/** Mensaje de outbox inmutable. Cada transición devuelve una copia nueva o un error tipado. */
public record OutboxMessage(
    MessageId id,
    String aggregateId,
    String type,
    Payload payload,
    int attempts,
    OutboxStatus status,
    Instant nextAttemptAt,
    Instant createdAt) {

  public OutboxMessage {
    Objects.requireNonNull(id, "id");
    Text.requireNonBlank(aggregateId, "aggregateId");
    Text.requireNonBlank(type, "type");
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(nextAttemptAt, "nextAttemptAt");
    Objects.requireNonNull(createdAt, "createdAt");
    if (attempts < 0) {
      throw new IllegalArgumentException("attempts must be >= 0");
    }
  }

  /** Mensaje nuevo, pendiente y listo para publicarse ya. */
  public static OutboxMessage pending(
      MessageId id, String aggregateId, String type, Payload payload, Instant now) {
    return new OutboxMessage(id, aggregateId, type, payload, 0, new Pending(), now, now);
  }

  public boolean isDue(Instant now) {
    return status instanceof Pending && !nextAttemptAt.isAfter(now);
  }

  public boolean isTerminal() {
    return status.isTerminal();
  }

  /** Pending y vencido → InFlight con un lease nuevo; cuenta un intento. */
  public Result<OutboxMessage> claim(RelayId owner, Instant now, Duration leaseDuration) {
    Objects.requireNonNull(owner, "owner");
    if (leaseDuration.isNegative() || leaseDuration.isZero()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    if (!(status instanceof Pending)) {
      return Result.err(new DomainError.NotClaimable(id, status));
    }
    if (nextAttemptAt.isAfter(now)) {
      return Result.err(new DomainError.NotDue(id, nextAttemptAt));
    }
    int attempt = attempts + 1;
    Lease lease = new Lease(owner, attempt, now.plus(leaseDuration));
    return Result.ok(with(attempt, new InFlight(lease), nextAttemptAt));
  }

  public Result<OutboxMessage> markPublished(Lease lease, Instant now) {
    DomainError error = checkLease(lease);
    if (error != null) {
      return Result.err(error);
    }
    return Result.ok(with(attempts, new Published(now, lease.owner()), nextAttemptAt));
  }

  /** Fallo transitorio: reintento con backoff, o cuarentena si se agotaron los intentos. */
  public Result<OutboxMessage> markTransientFailure(
      Lease lease, Instant now, String error, BackoffPolicy policy, LongUnaryOperator jitter) {
    DomainError stale = checkLease(lease);
    if (stale != null) {
      return Result.err(stale);
    }
    return Result.ok(retryOrQuarantine(now, error, policy, jitter));
  }

  /** Error permanente del publisher: cuarentena inmediata. */
  public Result<OutboxMessage> quarantine(Lease lease, Instant now, String reason) {
    DomainError stale = checkLease(lease);
    if (stale != null) {
      return Result.err(stale);
    }
    return Result.ok(
        with(attempts, new Quarantined(new PermanentFailure(reason), now), nextAttemptAt));
  }

  /** Lease vencido (el relay se cayó o tardó): cuenta como intento fallido. */
  public Result<OutboxMessage> releaseExpiredLease(
      Instant now, BackoffPolicy policy, LongUnaryOperator jitter) {
    if (!(status instanceof InFlight inFlight)) {
      return Result.err(new DomainError.NotClaimable(id, status));
    }
    if (inFlight.lease().isValidAt(now)) {
      return Result.err(new DomainError.LeaseStillValid(id, inFlight.lease().expiresAt()));
    }
    return Result.ok(retryOrQuarantine(now, "lease expired", policy, jitter));
  }

  private OutboxMessage retryOrQuarantine(
      Instant now, String error, BackoffPolicy policy, LongUnaryOperator jitter) {
    Objects.requireNonNull(error, "error");
    if (policy.exhausted(attempts)) {
      return with(
          attempts, new Quarantined(new MaxAttemptsExceeded(attempts, error), now), nextAttemptAt);
    }
    Instant next =
        now.plus(policy.delay(attempts, jitter.applyAsLong(policy.jitterBound(attempts))));
    return with(attempts, new Pending(), next);
  }

  private DomainError.@Nullable StaleLease checkLease(Lease lease) {
    Objects.requireNonNull(lease, "lease");
    if (status instanceof InFlight inFlight
        && inFlight.lease().owner().equals(lease.owner())
        && inFlight.lease().token() == lease.token()) {
      return null;
    }
    return new DomainError.StaleLease(id, lease.owner(), lease.token());
  }

  private OutboxMessage with(int newAttempts, OutboxStatus newStatus, Instant next) {
    return new OutboxMessage(
        id, aggregateId, type, payload, newAttempts, newStatus, next, createdAt);
  }
}
