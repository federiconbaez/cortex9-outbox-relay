package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.application.port.Clock;
import io.github.federiconbaez.outbox.application.port.Envelope;
import io.github.federiconbaez.outbox.application.port.OutboxStore;
import io.github.federiconbaez.outbox.application.port.PublishResult;
import io.github.federiconbaez.outbox.application.port.Publisher;
import io.github.federiconbaez.outbox.application.port.RandomSource;
import io.github.federiconbaez.outbox.application.port.Transition;
import io.github.federiconbaez.outbox.application.port.UpdateOutcome;
import io.github.federiconbaez.outbox.domain.BackoffPolicy;
import io.github.federiconbaez.outbox.domain.Lease;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.RelayId;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Relay del outbox: libera leases vencidos, reclama un lote, publica y marca. Entrega
 * at-least-once: un crash entre publicar y marcar produce una reentrega (ADR-0002).
 */
public final class Relay {
  private final RelayId id;
  private final OutboxStore store;
  private final Publisher publisher;
  private final Clock clock;
  private final RandomSource random;
  private final RelayConfig config;

  public Relay(
      RelayId id,
      OutboxStore store,
      Publisher publisher,
      Clock clock,
      RandomSource random,
      RelayConfig config) {
    this.id = Objects.requireNonNull(id);
    this.store = Objects.requireNonNull(store);
    this.publisher = Objects.requireNonNull(publisher);
    this.clock = Objects.requireNonNull(clock);
    this.random = Objects.requireNonNull(random);
    this.config = Objects.requireNonNull(config);
  }

  public RelayId id() {
    return id;
  }

  /** Libera leases vencidos y reclama un lote de mensajes vencidos. */
  public Claimed claim() {
    Instant now = clock.now();
    BackoffPolicy backoff = config.backoff();
    int released = 0;
    for (MessageId expired : store.expiredLeases(now)) {
      UpdateOutcome outcome =
          store.update(expired, m -> m.releaseExpiredLease(now, backoff, random::nextLong));
      if (outcome instanceof UpdateOutcome.Updated) {
        released++;
      }
    }
    return new Claimed(
        released, store.claimDue(id, now, config.leaseDuration(), config.batchSize()));
  }

  /** Publica un mensaje reclamado por este relay y registra el resultado. */
  public ProcessOutcome process(OutboxMessage claimed) {
    if (!(claimed.status() instanceof OutboxStatus.InFlight inFlight)
        || !inFlight.lease().owner().equals(id)) {
      throw new IllegalArgumentException("message " + claimed.id() + " is not claimed by " + id);
    }
    Lease lease = inFlight.lease();
    if (!lease.isValidAt(clock.now())) {
      return ProcessOutcome.LEASE_EXPIRED_BEFORE_PUBLISH;
    }
    PublishResult result = publishSafely(Envelope.of(claimed));
    Instant now = clock.now();
    BackoffPolicy backoff = config.backoff();
    Transition transition =
        switch (result) {
          case PublishResult.Ack ack -> m -> m.markPublished(lease, now);
          case PublishResult.Transient t ->
              m -> m.markTransientFailure(lease, now, t.reason(), backoff, random::nextLong);
          case PublishResult.Permanent p -> m -> m.quarantine(lease, now, p.reason());
        };
    return switch (store.update(claimed.id(), transition)) {
      case UpdateOutcome.Updated u -> classify(u.message());
      case UpdateOutcome.Rejected r -> ProcessOutcome.LEASE_LOST;
      case UpdateOutcome.NotFound n ->
          throw new IllegalStateException("claimed message disappeared: " + n.id());
    };
  }

  /** Un ciclo completo: reclamar y procesar el lote. */
  public RelayReport runOnce() {
    Claimed claimed = claim();
    Map<ProcessOutcome, Integer> outcomes = new EnumMap<>(ProcessOutcome.class);
    for (OutboxMessage message : claimed.messages()) {
      outcomes.merge(process(message), 1, Integer::sum);
    }
    return new RelayReport(claimed.released(), claimed.messages().size(), outcomes);
  }

  private PublishResult publishSafely(Envelope envelope) {
    try {
      return publisher.publish(envelope);
    } catch (RuntimeException e) {
      return new PublishResult.Transient("publisher threw " + e.getClass().getSimpleName());
    }
  }

  private static ProcessOutcome classify(OutboxMessage message) {
    return switch (message.status()) {
      case OutboxStatus.Published p -> ProcessOutcome.PUBLISHED;
      case OutboxStatus.Pending p -> ProcessOutcome.RETRY_SCHEDULED;
      case OutboxStatus.Quarantined q -> ProcessOutcome.QUARANTINED;
      case OutboxStatus.InFlight f ->
          throw new IllegalStateException("transition left message in flight: " + message.id());
    };
  }
}
