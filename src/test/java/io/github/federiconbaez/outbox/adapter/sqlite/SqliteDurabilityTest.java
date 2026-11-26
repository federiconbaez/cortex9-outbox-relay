package io.github.federiconbaez.outbox.adapter.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.adapter.fake.ScriptedPublisher;
import io.github.federiconbaez.outbox.adapter.fake.Step;
import io.github.federiconbaez.outbox.adapter.inmemory.ManualClock;
import io.github.federiconbaez.outbox.adapter.inmemory.SeededRandomSource;
import io.github.federiconbaez.outbox.adapter.inmemory.SequentialIdGenerator;
import io.github.federiconbaez.outbox.application.IdempotentConsumer;
import io.github.federiconbaez.outbox.application.NewMessage;
import io.github.federiconbaez.outbox.application.OutboxWriter;
import io.github.federiconbaez.outbox.application.Relay;
import io.github.federiconbaez.outbox.application.RelayConfig;
import io.github.federiconbaez.outbox.domain.BackoffPolicy;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.domain.RelayId;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Lo que el fake en memoria no puede probar: persistencia y exclusión entre conexiones. */
class SqliteDurabilityTest {
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final Duration LEASE = Duration.ofSeconds(30);
  private static final RelayConfig CONFIG =
      new RelayConfig(
          10, LEASE, new BackoffPolicy(Duration.ofSeconds(1), Duration.ofSeconds(8), 3));

  @TempDir Path dir;

  private static OutboxMessage message(String id) {
    return OutboxMessage.pending(new MessageId(id), "agg", "T", Payload.of(new byte[] {7}), T0);
  }

  @Test
  void committedStateSurvivesCloseAndReopen() {
    Path file = dir.resolve("outbox.db");
    try (SqliteOutboxDatabase db = SqliteOutboxDatabase.open(file)) {
      db.inTransaction(
          tx -> {
            tx.putRecord("order-1", "PLACED");
            tx.appendOutbox(message("m-1"));
            tx.appendOutbox(message("m-2"));
            return null;
          });
      db.claimDue(new RelayId("a"), T0, LEASE, 1);
    }
    try (SqliteOutboxDatabase db = SqliteOutboxDatabase.open(file)) {
      assertThat(db.records()).containsExactlyEntriesOf(Map.of("order-1", "PLACED"));
      assertThat(db.snapshot()).extracting(m -> m.id().value()).containsExactly("m-1", "m-2");
      assertThat(db.find(new MessageId("m-1")).orElseThrow().status())
          .isInstanceOf(OutboxStatus.InFlight.class);
      assertThat(db.expiredLeases(T0.plus(LEASE))).containsExactly(new MessageId("m-1"));
    }
  }

  @Test
  void twoConnectionsOnTheSameFileNeverClaimTheSameMessage() throws Exception {
    Path file = dir.resolve("shared.db");
    int messages = 300;
    try (SqliteOutboxDatabase writer = SqliteOutboxDatabase.open(file)) {
      writer.inTransaction(
          tx -> {
            for (int i = 0; i < messages; i++) {
              tx.appendOutbox(message("m-" + i));
            }
            return null;
          });
    }
    // Dos "procesos" (dos conexiones independientes al mismo archivo) con dos hilos cada uno.
    try (SqliteOutboxDatabase p1 = SqliteOutboxDatabase.open(file);
        SqliteOutboxDatabase p2 = SqliteOutboxDatabase.open(file)) {
      ExecutorService pool = Executors.newFixedThreadPool(4);
      CountDownLatch start = new CountDownLatch(1);
      List<Future<List<MessageId>>> futures = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        SqliteOutboxDatabase db = t % 2 == 0 ? p1 : p2;
        RelayId owner = new RelayId("relay-" + t);
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  List<MessageId> mine = new ArrayList<>();
                  List<OutboxMessage> batch;
                  while (!(batch = db.claimDue(owner, T0, LEASE, 5)).isEmpty()) {
                    batch.forEach(m -> mine.add(m.id()));
                  }
                  return mine;
                }));
      }
      start.countDown();
      List<MessageId> all = new ArrayList<>();
      for (Future<List<MessageId>> f : futures) {
        all.addAll(f.get(60, TimeUnit.SECONDS));
      }
      pool.shutdown();
      assertThat(all).hasSize(messages);
      assertThat(new HashSet<>(all)).hasSize(messages);
    }
  }

  @Test
  void relayResumesAfterRestartAndTheConsumerAppliesEachEffectOnce() {
    Path outboxFile = dir.resolve("outbox.db");
    Path dedupeFile = dir.resolve("consumer.db");
    ManualClock clock = new ManualClock(T0);
    Map<MessageId, Integer> effects = new HashMap<>();
    List<MessageId> ids;

    try (SqliteOutboxDatabase db = SqliteOutboxDatabase.open(outboxFile);
        SqliteDedupeStore dedupe = SqliteDedupeStore.open(dedupeFile)) {
      OutboxWriter writer = new OutboxWriter(db, clock, new SequentialIdGenerator("msg-"));
      ids =
          writer.commit(
              tx -> tx.putRecord("order-1", "PLACED"),
              List.of(
                  new NewMessage("order-1", "A", Payload.of(new byte[] {1})),
                  new NewMessage("order-1", "B", Payload.of(new byte[] {2}))));
      IdempotentConsumer consumer =
          new IdempotentConsumer(dedupe, e -> effects.merge(e.messageId(), 1, Integer::sum));
      ScriptedPublisher publisher =
          new ScriptedPublisher(consumer)
              .script(ids.get(0), Step.DELIVER_THEN_TRANSIENT, Step.ACK)
              .script(ids.get(1), Step.TRANSIENT, Step.ACK);
      Relay relay =
          new Relay(new RelayId("r1"), db, publisher, clock, new SeededRandomSource(1), CONFIG);
      relay.runOnce();
      assertThat(db.snapshot())
          .allSatisfy(m -> assertThat(m.status()).isEqualTo(new OutboxStatus.Pending()));
    }

    // "Reinicio": todo se reabre desde disco con objetos nuevos.
    clock.advance(Duration.ofSeconds(10));
    try (SqliteOutboxDatabase db = SqliteOutboxDatabase.open(outboxFile);
        SqliteDedupeStore dedupe = SqliteDedupeStore.open(dedupeFile)) {
      assertThat(dedupe.seen(ids.get(0))).isTrue();
      IdempotentConsumer consumer =
          new IdempotentConsumer(dedupe, e -> effects.merge(e.messageId(), 1, Integer::sum));
      Relay relay =
          new Relay(
              new RelayId("r2"),
              db,
              new ScriptedPublisher(consumer),
              clock,
              new SeededRandomSource(2),
              CONFIG);
      relay.runOnce();
      assertThat(db.snapshot())
          .allSatisfy(m -> assertThat(m.status()).isInstanceOf(OutboxStatus.Published.class));
      assertThat(db.records()).containsEntry("order-1", "PLACED");
    }
    assertThat(effects).containsExactlyInAnyOrderEntriesOf(Map.of(ids.get(0), 1, ids.get(1), 1));
  }

  @Test
  void corruptRowIsAStorageErrorAndTheTransactionIsRolledBack() throws Exception {
    Path file = dir.resolve("corrupt.db");
    try (SqliteOutboxDatabase db = SqliteOutboxDatabase.open(file)) {
      db.inTransaction(
          tx -> {
            tx.appendOutbox(message("m-1"));
            tx.appendOutbox(message("m-2"));
            return null;
          });
      try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + file);
          Statement s = c.createStatement()) {
        s.execute("UPDATE outbox SET status = 'IN_FLIGHT' WHERE id = 'm-1'");
      }
      assertThatThrownBy(() -> db.find(new MessageId("m-1")))
          .isInstanceOf(StorageException.class)
          .hasMessageContaining("lease_owner");
      // Si el rollback no hubiera ocurrido, la conexión quedaría dentro de una transacción.
      assertThat(db.find(new MessageId("m-2"))).contains(message("m-2"));
      assertThat(db.claimDue(new RelayId("a"), T0, LEASE, 10))
          .extracting(m -> m.id().value())
          .containsExactly("m-2");
    }
  }

  @Test
  void sqlFailureInsideATransactionIsAStorageErrorAndRollsBack() throws Exception {
    Path file = dir.resolve("dropped.db");
    try (SqliteOutboxDatabase db = SqliteOutboxDatabase.open(file)) {
      try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + file);
          Statement s = c.createStatement()) {
        s.execute("DROP TABLE business_record");
      }
      assertThatThrownBy(
              () ->
                  db.inTransaction(
                      tx -> {
                        tx.putRecord("k", "v");
                        tx.appendOutbox(message("m-1"));
                        return null;
                      }))
          .isInstanceOf(StorageException.class);
      assertThat(db.snapshot()).isEmpty();
    }
  }

  @Test
  void closedStoresRejectFurtherWork() {
    SqliteOutboxDatabase db = SqliteOutboxDatabase.open(dir.resolve("closed.db"));
    db.close();
    assertThatThrownBy(db::snapshot).isInstanceOf(StorageException.class);
    SqliteDedupeStore dedupe = SqliteDedupeStore.open(dir.resolve("closed-dedupe.db"));
    dedupe.close();
    assertThatThrownBy(() -> dedupe.seen(new MessageId("m-1")))
        .isInstanceOf(StorageException.class);
  }

  @Test
  void refusesASchemaNewerThanItKnows() throws Exception {
    Path file = dir.resolve("future.db");
    try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + file);
        Statement s = c.createStatement()) {
      s.execute("PRAGMA user_version = 99");
    }
    assertThatThrownBy(() -> SqliteOutboxDatabase.open(file))
        .isInstanceOf(StorageException.class)
        .hasMessageContaining("99");
  }

  @Test
  void unreadableFileIsAStorageError() throws Exception {
    Path file = dir.resolve("garbage.db");
    Files.writeString(file, "this is not a sqlite database, just text padding ".repeat(50));
    assertThatThrownBy(() -> SqliteOutboxDatabase.open(file)).isInstanceOf(StorageException.class);
  }
}
