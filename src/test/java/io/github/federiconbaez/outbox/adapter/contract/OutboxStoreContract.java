package io.github.federiconbaez.outbox.adapter.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.application.port.OutboxStore;
import io.github.federiconbaez.outbox.application.port.UnitOfWork;
import io.github.federiconbaez.outbox.application.port.UpdateOutcome;
import io.github.federiconbaez.outbox.domain.DomainError;
import io.github.federiconbaez.outbox.domain.Lease;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.domain.RelayId;
import io.github.federiconbaez.outbox.domain.Result;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Suite de contrato de {@link OutboxStore} + {@link UnitOfWork}. Cualquier adapter (JDBC, etc.)
 * debería extenderla y pasarla sin cambios.
 */
public abstract class OutboxStoreContract {
  protected static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  protected static final Duration LEASE = Duration.ofSeconds(30);
  protected static final RelayId A = new RelayId("a");
  protected static final RelayId B = new RelayId("b");

  public record Subject(OutboxStore store, UnitOfWork unitOfWork) {}

  protected abstract Subject create();

  private OutboxStore store;
  private UnitOfWork uow;

  @BeforeEach
  void setUp() {
    Subject s = create();
    store = s.store();
    uow = s.unitOfWork();
  }

  private static OutboxMessage message(String id, Instant at) {
    return OutboxMessage.pending(new MessageId(id), "agg", "T", Payload.of(new byte[] {1}), at);
  }

  private void commit(OutboxMessage... messages) {
    uow.inTransaction(
        tx -> {
          for (OutboxMessage m : messages) {
            tx.appendOutbox(m);
          }
          return null;
        });
  }

  @Test
  void onlyCommittedMessagesAreVisible() {
    assertThatThrownBy(
            () ->
                uow.inTransaction(
                    tx -> {
                      tx.appendOutbox(message("m-1", T0));
                      throw new IllegalStateException("rollback");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(store.snapshot()).isEmpty();
    assertThat(store.find(new MessageId("m-1"))).isEmpty();
    commit(message("m-1", T0));
    assertThat(store.find(new MessageId("m-1"))).contains(message("m-1", T0));
  }

  @Test
  void onlyPendingMessagesCanBeAppended() {
    OutboxMessage claimed =
        ((Result.Ok<OutboxMessage>) message("m-1", T0).claim(A, T0, LEASE)).value();
    assertThatThrownBy(
            () ->
                uow.inTransaction(
                    tx -> {
                      tx.appendOutbox(claimed);
                      return null;
                    }))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(store.snapshot()).isEmpty();
  }

  @Test
  void transactionReturnsTheWorkResult() {
    Integer result = uow.inTransaction(tx -> 7);
    assertThat(result).isEqualTo(7);
  }

  @Test
  void duplicateIdsAreRejectedAtomically() {
    commit(message("m-1", T0));
    assertThatThrownBy(() -> commit(message("m-2", T0), message("m-1", T0)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(store.snapshot()).hasSize(1);
  }

  @Test
  void claimsDueMessagesInOrderUpToLimit() {
    commit(message("late", T0.plusSeconds(1)), message("m-1", T0), message("m-2", T0));
    commit(message("future", T0.plusSeconds(60)));
    List<OutboxMessage> claimed = store.claimDue(A, T0.plusSeconds(1), LEASE, 2);
    assertThat(claimed).extracting(m -> m.id().value()).containsExactly("m-1", "m-2");
    assertThat(claimed)
        .allSatisfy(
            m ->
                assertThat(m.status())
                    .isEqualTo(
                        new OutboxStatus.InFlight(new Lease(A, 1, T0.plusSeconds(1).plus(LEASE)))));
    assertThat(store.claimDue(B, T0.plusSeconds(1), LEASE, 10))
        .extracting(m -> m.id().value())
        .containsExactly("late");
    assertThat(store.claimDue(B, T0.plusSeconds(1), LEASE, 10)).isEmpty();
    assertThat(store.find(new MessageId("m-1")).orElseThrow().status())
        .isInstanceOf(OutboxStatus.InFlight.class);
  }

  @Test
  void rejectsNonPositiveLimit() {
    assertThatThrownBy(() -> store.claimDue(A, T0, LEASE, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void updateAppliesTransitionOrReportsRejectionAndMissing() {
    commit(message("m-1", T0));
    OutboxMessage claimed = store.claimDue(A, T0, LEASE, 1).get(0);
    Lease lease = ((OutboxStatus.InFlight) claimed.status()).lease();
    Lease stale = new Lease(B, 1, lease.expiresAt());

    UpdateOutcome rejected = store.update(claimed.id(), m -> m.markPublished(stale, T0));
    assertThat(rejected)
        .isEqualTo(new UpdateOutcome.Rejected(new DomainError.StaleLease(claimed.id(), B, 1)));
    assertThat(store.find(claimed.id())).contains(claimed);

    UpdateOutcome updated = store.update(claimed.id(), m -> m.markPublished(lease, T0));
    assertThat(updated).isInstanceOf(UpdateOutcome.Updated.class);
    assertThat(store.find(claimed.id()).orElseThrow().status())
        .isEqualTo(new OutboxStatus.Published(T0, A));

    MessageId missing = new MessageId("nope");
    assertThat(store.update(missing, Result::ok)).isEqualTo(new UpdateOutcome.NotFound(missing));
  }

  @Test
  void transitionCannotChangeTheMessageId() {
    commit(message("m-1", T0), message("m-2", T0));
    OutboxMessage other = store.find(new MessageId("m-2")).orElseThrow();
    assertThatThrownBy(() -> store.update(new MessageId("m-1"), m -> Result.ok(other)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(store.find(new MessageId("m-1"))).contains(message("m-1", T0));
  }

  @Test
  void listsOnlyExpiredLeases() {
    commit(message("m-1", T0), message("m-2", T0), message("m-3", T0));
    store.claimDue(A, T0, LEASE, 1);
    store.claimDue(B, T0.plusSeconds(10), LEASE, 1);
    assertThat(store.expiredLeases(T0.plus(LEASE).minusMillis(1))).isEmpty();
    assertThat(store.expiredLeases(T0.plus(LEASE))).containsExactly(new MessageId("m-1"));
    assertThat(store.expiredLeases(T0.plusSeconds(100)))
        .containsExactlyInAnyOrder(new MessageId("m-1"), new MessageId("m-2"));
  }

  @Test
  void dueTimeIsComparedWithNanosecondPrecision() {
    commit(message("b", T0.plusNanos(500)), message("a", T0.plusNanos(1)));
    assertThat(store.claimDue(A, T0, LEASE, 10)).isEmpty();
    assertThat(store.claimDue(A, T0.plusNanos(499), LEASE, 10))
        .extracting(m -> m.id().value())
        .containsExactly("a");
    assertThat(store.claimDue(A, T0.plusNanos(500), LEASE, 10))
        .extracting(m -> m.id().value())
        .containsExactly("b");
  }

  @Test
  void snapshotKeepsInsertionOrderAcrossUpdates() {
    commit(message("z", T0), message("a", T0));
    commit(message("m", T0));
    store.claimDue(A, T0, LEASE, 1);
    assertThat(store.snapshot()).extracting(m -> m.id().value()).containsExactly("z", "a", "m");
    assertThat(store.snapshot().get(0).status()).isInstanceOf(OutboxStatus.InFlight.class);
  }

  @Test
  void concurrentClaimsNeverReturnTheSameMessage() throws Exception {
    int messages = 500;
    for (int i = 0; i < messages; i++) {
      commit(message("m-" + i, T0));
    }
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<List<MessageId>>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      RelayId owner = new RelayId("relay-" + t);
      futures.add(
          pool.submit(
              () -> {
                start.await();
                List<MessageId> mine = new ArrayList<>();
                List<OutboxMessage> batch;
                while (!(batch = store.claimDue(owner, T0, LEASE, 7)).isEmpty()) {
                  batch.forEach(m -> mine.add(m.id()));
                }
                return mine;
              }));
    }
    start.countDown();
    List<MessageId> all = Collections.synchronizedList(new ArrayList<>());
    for (Future<List<MessageId>> f : futures) {
      all.addAll(f.get(30, TimeUnit.SECONDS));
    }
    pool.shutdown();
    Set<MessageId> unique = new HashSet<>(all);
    assertThat(all).hasSize(messages);
    assertThat(unique).hasSize(messages);
  }
}
