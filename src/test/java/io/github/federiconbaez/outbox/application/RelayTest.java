package io.github.federiconbaez.outbox.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.adapter.fake.Step;
import io.github.federiconbaez.outbox.adapter.inmemory.SimulatedCrash;
import io.github.federiconbaez.outbox.domain.BackoffPolicy;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.QuarantineReason;
import io.github.federiconbaez.outbox.domain.RelayId;
import io.github.federiconbaez.outbox.support.World;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class RelayTest {
  private final World w = new World();

  @Test
  void publishesCommittedMessagesAndConsumerAppliesThem() {
    List<MessageId> ids = w.write(3);
    RelayReport report = w.relay("r1").runOnce();
    assertThat(report.claimed()).isEqualTo(3);
    assertThat(report.count(ProcessOutcome.PUBLISHED)).isEqualTo(3);
    assertThat(report.released()).isZero();
    for (MessageId id : ids) {
      assertThat(w.get(id).status())
          .isEqualTo(new OutboxStatus.Published(World.T0, new RelayId("r1")));
      assertThat(w.effectCount(id)).isEqualTo(1);
    }
    assertThat(w.relay("r1").runOnce().claimed()).isZero();
  }

  @Test
  void respectsBatchSize() {
    World small =
        new World(1, new BackoffPolicy(Duration.ofMillis(100), Duration.ofSeconds(5), 3), 2);
    small.write(5);
    assertThat(small.relay("r").runOnce().claimed()).isEqualTo(2);
  }

  @Test
  void transientFailureIsRetriedAfterBackoff() {
    MessageId id = w.write(1).get(0);
    w.publisher.script(id, Step.TRANSIENT);
    Relay relay = w.relay("r1");
    assertThat(relay.runOnce().count(ProcessOutcome.RETRY_SCHEDULED)).isEqualTo(1);
    OutboxMessage retry = w.get(id);
    assertThat(retry.status()).isEqualTo(new OutboxStatus.Pending());
    assertThat(Duration.between(World.T0, retry.nextAttemptAt()))
        .isBetween(Duration.ofMillis(50), Duration.ofMillis(100));
    assertThat(relay.runOnce().claimed()).isZero();
    w.clock.advance(Duration.between(World.T0, retry.nextAttemptAt()));
    assertThat(relay.runOnce().count(ProcessOutcome.PUBLISHED)).isEqualTo(1);
    assertThat(w.get(id).attempts()).isEqualTo(2);
  }

  @Test
  void exhaustedTransientFailuresGoToQuarantine() {
    MessageId id = w.write(1).get(0);
    w.publisher.script(id, Step.TRANSIENT, Step.THROW, Step.TRANSIENT, Step.ACK);
    Relay relay = w.relay("r1");
    for (int i = 0; i < 3; i++) {
      relay.runOnce();
      w.clock.advance(Duration.ofSeconds(5));
    }
    assertThat(w.get(id).status())
        .isEqualTo(
            new OutboxStatus.Quarantined(
                new QuarantineReason.MaxAttemptsExceeded(3, "scripted transient"),
                World.T0.plusSeconds(10)));
    assertThat(w.effectCount(id)).isZero();
  }

  @Test
  void publisherExceptionIsTreatedAsTransient() {
    MessageId id = w.write(1).get(0);
    w.publisher.script(id, Step.THROW);
    assertThat(w.relay("r1").runOnce().count(ProcessOutcome.RETRY_SCHEDULED)).isEqualTo(1);
    assertThat(w.get(id).status()).isInstanceOf(OutboxStatus.Pending.class);
  }

  @Test
  void permanentFailureQuarantinesWithoutRetry() {
    MessageId id = w.write(1).get(0);
    w.publisher.script(id, Step.PERMANENT);
    assertThat(w.relay("r1").runOnce().count(ProcessOutcome.QUARANTINED)).isEqualTo(1);
    assertThat(w.get(id).status())
        .isEqualTo(
            new OutboxStatus.Quarantined(
                new QuarantineReason.PermanentFailure("scripted permanent"), World.T0));
    assertThat(w.publisher.calls()).isEqualTo(1);
  }

  @Test
  void doesNotPublishWhenLeaseExpiredBeforePublishing() {
    w.write(1);
    Relay relay = w.relay("slow");
    OutboxMessage claimed = relay.claim().messages().get(0);
    w.clock.advance(World.LEASE);
    assertThat(relay.process(claimed)).isEqualTo(ProcessOutcome.LEASE_EXPIRED_BEFORE_PUBLISH);
    assertThat(w.publisher.calls()).isZero();
  }

  @Test
  void slowRelayLosesItsLeaseToAnotherRelay() {
    MessageId id = w.write(1).get(0);
    w.publisher.script(id, Step.ACK);
    Relay slow = w.relay("slow");
    OutboxMessage claimedBySlow = slow.claim().messages().get(0);
    w.clock.advance(World.LEASE);
    Relay fast = w.relay("fast");
    Claimed claimedByFast = fast.claim();
    assertThat(claimedByFast.released()).isEqualTo(1);
    assertThat(claimedByFast.messages()).isEmpty();
    w.clock.advance(Duration.ofSeconds(5));
    claimedByFast = fast.claim();
    assertThat(claimedByFast.messages()).hasSize(1);
    assertThat(fast.process(claimedByFast.messages().get(0))).isEqualTo(ProcessOutcome.PUBLISHED);
    assertThat(w.get(id).status()).isInstanceOf(OutboxStatus.Published.class);
    assertThatThrownBy(() -> fast.process(claimedBySlow))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void staleMarkIsRejectedByFencing() {
    MessageId id = w.write(1).get(0);
    w.publisher.script(id, Step.DELIVER_THEN_TRANSIENT);
    Relay a = w.relay("a");
    OutboxMessage claimedByA = a.claim().messages().get(0);
    // A queda pausado; su lease vence, B libera, reclama y publica.
    w.clock.advance(World.LEASE.plusSeconds(10));
    Relay b = w.relay("b");
    Claimed released = b.claim();
    assertThat(released.released()).isEqualTo(1);
    w.clock.advance(Duration.ofSeconds(5));
    OutboxMessage claimedByB = b.claim().messages().get(0);
    assertThat(b.process(claimedByB)).isEqualTo(ProcessOutcome.RETRY_SCHEDULED);
    // A "despierta" con un lease viejo pero vigente según su copia: se le fuerza la marca.
    Relay aAgain =
        new Relay(
            new RelayId("a"),
            w.db,
            env -> new io.github.federiconbaez.outbox.application.port.PublishResult.Ack(),
            () -> World.T0,
            w.random,
            w.config);
    assertThat(aAgain.process(claimedByA)).isEqualTo(ProcessOutcome.LEASE_LOST);
    assertThat(w.get(id).status()).isInstanceOf(OutboxStatus.Pending.class);
    assertThat(w.effectCount(id)).isEqualTo(1);
  }

  @Test
  void crashBetweenPublishAndMarkCausesRedeliveryButSingleEffect() {
    MessageId id = w.write(1).get(0);
    w.db.crashBeforeNextUpdate();
    assertThatThrownBy(() -> w.relay("r1").runOnce()).isInstanceOf(SimulatedCrash.class);
    assertThat(w.get(id).status()).isInstanceOf(OutboxStatus.InFlight.class);
    assertThat(w.effectCount(id)).isEqualTo(1);

    w.clock.advance(World.LEASE);
    Relay r2 = w.relay("r2");
    RelayReport first = r2.runOnce();
    assertThat(first.released()).isEqualTo(1);
    w.clock.advance(Duration.ofSeconds(5));
    assertThat(r2.runOnce().count(ProcessOutcome.PUBLISHED)).isEqualTo(1);
    assertThat(w.publisher.delivered()).hasSize(2);
    assertThat(w.effectCount(id)).isEqualTo(1);
  }

  @Test
  void rejectsInvalidConfig() {
    BackoffPolicy p = new BackoffPolicy(Duration.ofMillis(1), Duration.ofMillis(1), 1);
    assertThatThrownBy(() -> new RelayConfig(0, Duration.ofSeconds(1), p))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RelayConfig(1, Duration.ZERO, p))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(w.relay("x").id()).isEqualTo(new RelayId("x"));
  }
}
