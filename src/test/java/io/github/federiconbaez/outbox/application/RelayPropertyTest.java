package io.github.federiconbaez.outbox.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.federiconbaez.outbox.adapter.fake.Step;
import io.github.federiconbaez.outbox.adapter.inmemory.SimulatedCrash;
import io.github.federiconbaez.outbox.application.port.Envelope;
import io.github.federiconbaez.outbox.domain.BackoffPolicy;
import io.github.federiconbaez.outbox.domain.Lease;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.QuarantineReason;
import io.github.federiconbaez.outbox.support.World;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.statistics.Statistics;

class RelayPropertyTest {
  private static final int MAX_ATTEMPTS = 4;
  private static final BackoffPolicy POLICY =
      new BackoffPolicy(Duration.ofMillis(100), Duration.ofSeconds(2), MAX_ATTEMPTS);

  /** Guion de fallos de un mensaje: k fallos transitorios y opcionalmente uno permanente. */
  record Script(List<Step> transients, boolean permanent) {}

  @Provide
  Arbitrary<List<Script>> scripts() {
    Arbitrary<List<Step>> transients =
        Arbitraries.of(Step.TRANSIENT, Step.THROW, Step.DELIVER_THEN_TRANSIENT)
            .list()
            .ofMaxSize(MAX_ATTEMPTS + 1);
    return Combinators.combine(transients, Arbitraries.of(true, false))
        .as(Script::new)
        .list()
        .ofMinSize(1)
        .ofMaxSize(15);
  }

  /**
   * Garantía 2: con fallos transitorios acotados, todo mensaje confirmado termina Published o
   * Quarantined, y exactamente en el estado que predice su guion.
   */
  @Property(tries = 200)
  void everyCommittedMessageEventuallyPublishedOrQuarantined(
      @ForAll("scripts") List<Script> scripts, @ForAll long seed) {
    World w = new World(seed, POLICY, 4);
    List<MessageId> ids = w.write(scripts.size());
    for (int i = 0; i < ids.size(); i++) {
      Script s = scripts.get(i);
      w.publisher.script(ids.get(i), s.transients().toArray(Step[]::new));
      if (s.permanent()) {
        w.publisher.script(ids.get(i), Step.PERMANENT);
      }
    }
    Relay relay = w.relay("r");
    int rounds = 0;
    while (w.db.snapshot().stream().anyMatch(m -> !m.isTerminal())) {
      relay.runOnce();
      w.clock.advance(POLICY.cap());
      assertThat(++rounds).as("termination bound").isLessThan(500);
    }
    for (int i = 0; i < ids.size(); i++) {
      Script s = scripts.get(i);
      OutboxStatus status = w.get(ids.get(i)).status();
      int failures = s.transients().size();
      if (failures >= MAX_ATTEMPTS) {
        assertThat(status)
            .isInstanceOfSatisfying(
                OutboxStatus.Quarantined.class,
                q ->
                    assertThat(q.reason())
                        .isInstanceOf(QuarantineReason.MaxAttemptsExceeded.class));
      } else if (s.permanent()) {
        assertThat(status)
            .isInstanceOfSatisfying(
                OutboxStatus.Quarantined.class,
                q -> assertThat(q.reason()).isInstanceOf(QuarantineReason.PermanentFailure.class));
      } else {
        assertThat(status).isInstanceOf(OutboxStatus.Published.class);
        assertThat(w.effectCount(ids.get(i))).isEqualTo(1);
      }
      assertThat(w.effectCount(ids.get(i))).isLessThanOrEqualTo(1);
    }
    Statistics.collect(rounds < 5 ? "<5 rounds" : ">=5 rounds");
  }

  sealed interface Action {}

  record Claim(int relay) implements Action {}

  record Process(int relay) implements Action {}

  record ProcessAndCrash(int relay) implements Action {}

  record Forget(int relay) implements Action {}

  record Advance(long millis) implements Action {}

  @Provide
  Arbitrary<List<Action>> actions() {
    Arbitrary<Integer> relay = Arbitraries.integers().between(0, 2);
    Arbitrary<Action> action =
        Arbitraries.oneOf(
            relay.map(Claim::new),
            relay.map(Process::new),
            relay.map(Process::new),
            relay.map(ProcessAndCrash::new),
            relay.map(Forget::new),
            Arbitraries.longs().between(0, 45_000).map(Advance::new));
    return action.list().ofMinSize(5).ofMaxSize(80);
  }

  /** Una llamada al publisher con el lease vigente en el store en ese instante. */
  record Delivery(MessageId id, Lease lease, Instant at) {}

  record Claimant(MessageId id, int token) {}

  /**
   * Garantía 3: con varios relays intercalados, pausas y crashes, nunca hay dos publicaciones del
   * mismo mensaje dentro de un lease activo: cada publicación usa un token mayor y ocurre recién
   * cuando el lease anterior terminó, sea porque su dueño marcó el resultado o porque venció.
   */
  @Property(tries = 300)
  void noMessagePublishedByTwoRelaysWithinALease(
      @ForAll("actions") List<Action> actions,
      @ForAll @IntRange(min = 1, max = 6) int messages,
      @ForAll long seed) {
    World w = new World(seed, POLICY, 2);
    List<MessageId> ids = w.write(messages);
    ids.forEach(id -> w.publisher.script(id, Step.DELIVER_THEN_TRANSIENT));
    List<Delivery> deliveries = new ArrayList<>();
    Map<Claimant, Instant> finishedAt = new HashMap<>();
    List<Relay> relays = new ArrayList<>();
    List<Deque<OutboxMessage>> queues = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      relays.add(
          w.relay(
              "r" + i,
              w.observing(
                  e -> {
                    OutboxStatus st = w.get(e.messageId()).status();
                    Lease lease = ((OutboxStatus.InFlight) st).lease();
                    deliveries.add(new Delivery(e.messageId(), lease, w.clock.now()));
                    return null;
                  })));
      queues.add(new ArrayDeque<>());
    }
    for (Action a : actions) {
      switch (a) {
        case Claim c -> queues.get(c.relay()).addAll(relays.get(c.relay()).claim().messages());
        case Process p -> {
          OutboxMessage m = queues.get(p.relay()).poll();
          if (m != null) {
            int token = ((OutboxStatus.InFlight) m.status()).lease().token();
            ProcessOutcome outcome = relays.get(p.relay()).process(m);
            if (outcome != ProcessOutcome.LEASE_LOST
                && outcome != ProcessOutcome.LEASE_EXPIRED_BEFORE_PUBLISH) {
              finishedAt.put(new Claimant(m.id(), token), w.clock.now());
            }
          }
        }
        case ProcessAndCrash p -> {
          OutboxMessage m = queues.get(p.relay()).poll();
          // Solo se arma el crash si el relay va a publicar (lease vigente) y por ende a marcar.
          if (m != null && ((OutboxStatus.InFlight) m.status()).lease().isValidAt(w.clock.now())) {
            w.db.crashBeforeNextUpdate();
            try {
              relays.get(p.relay()).process(m);
            } catch (SimulatedCrash expected) {
              queues.get(p.relay()).clear();
            }
          }
        }
        case Forget f -> queues.get(f.relay()).clear();
        case Advance adv -> w.clock.advance(Duration.ofMillis(adv.millis()));
      }
    }
    Map<MessageId, List<Delivery>> byMessage = new HashMap<>();
    deliveries.forEach(d -> byMessage.computeIfAbsent(d.id(), k -> new ArrayList<>()).add(d));
    for (List<Delivery> ds : byMessage.values()) {
      for (int i = 0; i < ds.size(); i++) {
        assertThat(ds.get(i).at()).isBefore(ds.get(i).lease().expiresAt());
        if (i > 0) {
          Delivery prev = ds.get(i - 1);
          Delivery next = ds.get(i);
          assertThat(next.lease().token()).isGreaterThan(prev.lease().token());
          Instant marked =
              finishedAt.getOrDefault(
                  new Claimant(prev.id(), prev.lease().token()), prev.lease().expiresAt());
          Instant prevEnd =
              marked.isBefore(prev.lease().expiresAt()) ? marked : prev.lease().expiresAt();
          assertThat(next.at()).isAfterOrEqualTo(prevEnd);
        }
      }
    }
    for (MessageId id : ids) {
      assertThat(w.effectCount(id)).isLessThanOrEqualTo(1);
    }
    Statistics.collect(
        byMessage.values().stream().anyMatch(ds -> ds.size() > 1) ? "redelivery" : "no redelivery");
  }

  /**
   * Garantía 4: crashes entre publicar y marcar producen entregas duplicadas, pero el consumidor
   * idempotente aplica exactamente un efecto por mensaje.
   */
  @Property(tries = 200)
  void crashAfterPublishDuplicatesDeliveryButNotEffect(
      @ForAll @IntRange(min = 1, max = 20) int messages,
      @ForAll @Size(max = 40) List<Boolean> crashPlan,
      @ForAll long seed) {
    BackoffPolicy generous = new BackoffPolicy(Duration.ofMillis(10), Duration.ofSeconds(1), 1_000);
    World w = new World(seed, generous, 5);
    List<MessageId> ids = w.write(messages);
    Deque<Boolean> plan = new ArrayDeque<>(crashPlan);
    int rounds = 0;
    int crashes = 0;
    while (w.db.snapshot().stream().anyMatch(m -> !m.isTerminal())) {
      Boolean crash = plan.poll();
      if (Boolean.TRUE.equals(crash)) {
        w.db.crashBeforeNextUpdate();
      }
      try {
        w.relay("relay-" + rounds).runOnce();
      } catch (SimulatedCrash expected) {
        crashes++;
      }
      w.clock.advance(World.LEASE);
      assertThat(++rounds).as("termination bound").isLessThan(500);
    }
    for (MessageId id : ids) {
      assertThat(w.get(id).status()).isInstanceOf(OutboxStatus.Published.class);
      assertThat(w.effectCount(id)).isEqualTo(1);
    }
    List<Envelope> delivered = w.publisher.delivered();
    assertThat(delivered.size()).isGreaterThanOrEqualTo(messages);
    Statistics.collect(
        delivered.size() > messages ? "duplicates delivered" : "no duplicates",
        crashes > 0 ? "crashed" : "no crash");
  }
}
