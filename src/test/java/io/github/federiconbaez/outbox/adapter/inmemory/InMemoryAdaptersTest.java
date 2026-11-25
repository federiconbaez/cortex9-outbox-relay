package io.github.federiconbaez.outbox.adapter.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.adapter.fake.ScriptedPublisher;
import io.github.federiconbaez.outbox.adapter.fake.Step;
import io.github.federiconbaez.outbox.application.port.Envelope;
import io.github.federiconbaez.outbox.application.port.PublishResult;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.domain.RelayId;
import io.github.federiconbaez.outbox.domain.Result;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryAdaptersTest {
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void crashBeforeCommitDiscardsBusinessChangeAndMessages() {
    InMemoryOutboxDatabase db = new InMemoryOutboxDatabase();
    db.crashBeforeNextCommit();
    assertThatThrownBy(
            () ->
                db.inTransaction(
                    tx -> {
                      tx.putRecord("k", "v");
                      tx.appendOutbox(
                          OutboxMessage.pending(
                              new MessageId("m"), "a", "t", Payload.of(new byte[0]), T0));
                      return null;
                    }))
        .isInstanceOf(SimulatedCrash.class)
        .hasMessageContaining("before commit");
    assertThat(db.records()).isEmpty();
    assertThat(db.snapshot()).isEmpty();
  }

  @Test
  void crashBeforeUpdateLeavesMessageUntouchedOnce() {
    InMemoryOutboxDatabase db = new InMemoryOutboxDatabase();
    OutboxMessage m =
        OutboxMessage.pending(new MessageId("m"), "a", "t", Payload.of(new byte[0]), T0);
    db.inTransaction(
        tx -> {
          tx.appendOutbox(m);
          return null;
        });
    db.crashBeforeNextUpdate();
    assertThatThrownBy(() -> db.update(m.id(), Result::ok)).isInstanceOf(SimulatedCrash.class);
    assertThat(db.update(m.id(), Result::ok)).isNotNull();
  }

  @Test
  void onlyPendingMessagesCanBeAppended() {
    InMemoryOutboxDatabase db = new InMemoryOutboxDatabase();
    OutboxMessage claimed =
        ((Result.Ok<OutboxMessage>)
                OutboxMessage.pending(new MessageId("m"), "a", "t", Payload.of(new byte[0]), T0)
                    .claim(new RelayId("r"), T0, Duration.ofSeconds(1)))
            .value();
    assertThatThrownBy(
            () ->
                db.inTransaction(
                    tx -> {
                      tx.appendOutbox(claimed);
                      return null;
                    }))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void clocksRandomAndIds() {
    ManualClock clock = new ManualClock(T0);
    clock.advance(Duration.ofSeconds(3));
    assertThat(clock.now()).isEqualTo(T0.plusSeconds(3));
    assertThatThrownBy(() -> clock.advance(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(new SystemClock().now()).isAfter(T0.minusSeconds(1));

    SeededRandomSource r1 = new SeededRandomSource(7);
    SeededRandomSource r2 = new SeededRandomSource(7);
    for (int i = 0; i < 20; i++) {
      long v = r1.nextLong(100);
      assertThat(v).isEqualTo(r2.nextLong(100)).isBetween(0L, 99L);
    }
    SequentialIdGenerator ids = new SequentialIdGenerator("x");
    assertThat(ids.next()).isEqualTo(new MessageId("x-1"));
    assertThat(ids.next()).isEqualTo(new MessageId("x-2"));
  }

  @Test
  void scriptedPublisherFollowsScriptThenAcks() {
    List<Envelope> downstream = new ArrayList<>();
    ScriptedPublisher p = new ScriptedPublisher(downstream::add);
    MessageId id = new MessageId("m");
    Envelope e = new Envelope(id, "a", "t", Payload.of(new byte[0]), 1);
    p.script(id, Step.TRANSIENT, Step.PERMANENT, Step.DELIVER_THEN_TRANSIENT, Step.THROW);
    assertThat(p.publish(e)).isInstanceOf(PublishResult.Transient.class);
    assertThat(p.publish(e)).isInstanceOf(PublishResult.Permanent.class);
    assertThat(p.publish(e)).isEqualTo(new PublishResult.Transient("ack lost"));
    assertThatThrownBy(() -> p.publish(e)).isInstanceOf(IllegalStateException.class);
    assertThat(p.publish(e)).isEqualTo(new PublishResult.Ack());
    assertThat(downstream).hasSize(2);
    assertThat(p.delivered()).hasSize(2);
    assertThat(p.calls()).isEqualTo(5);
  }
}
