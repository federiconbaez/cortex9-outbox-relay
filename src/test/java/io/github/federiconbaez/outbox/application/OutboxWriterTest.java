package io.github.federiconbaez.outbox.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.adapter.inmemory.SimulatedCrash;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.support.World;
import java.util.List;
import org.junit.jupiter.api.Test;

class OutboxWriterTest {
  private final World w = new World();

  @Test
  void businessChangeAndMessagesCommitTogether() {
    List<MessageId> ids =
        w.writer.commit(
            tx -> tx.putRecord("order-1", "PLACED"),
            List.of(
                new NewMessage("order-1", "OrderPlaced", Payload.of(new byte[] {1})),
                new NewMessage("order-1", "StockReserved", Payload.of(new byte[] {2}))));
    assertThat(ids).containsExactly(new MessageId("msg-1"), new MessageId("msg-2"));
    assertThat(w.db.records()).containsEntry("order-1", "PLACED");
    assertThat(w.db.snapshot())
        .extracting(m -> m.type(), m -> m.status(), m -> m.createdAt())
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple(
                "OrderPlaced", new OutboxStatus.Pending(), World.T0),
            org.assertj.core.groups.Tuple.tuple(
                "StockReserved", new OutboxStatus.Pending(), World.T0));
  }

  @Test
  void failingBusinessChangeWritesNothing() {
    assertThatThrownBy(
            () ->
                w.writer.commit(
                    tx -> {
                      tx.putRecord("order-1", "PLACED");
                      throw new IllegalStateException("invariant violated");
                    },
                    List.of(new NewMessage("order-1", "OrderPlaced", Payload.of(new byte[0])))))
        .isInstanceOf(IllegalStateException.class);
    assertThat(w.db.records()).isEmpty();
    assertThat(w.db.snapshot()).isEmpty();
  }

  @Test
  void crashBeforeCommitLeavesNoOrphanMessages() {
    w.db.crashBeforeNextCommit();
    assertThatThrownBy(() -> w.write(3)).isInstanceOf(SimulatedCrash.class);
    assertThat(w.db.records()).isEmpty();
    assertThat(w.db.snapshot()).isEmpty();
    w.write(1);
    assertThat(w.db.snapshot()).hasSize(1);
  }

  @Test
  void newMessageRejectsNulls() {
    assertThatThrownBy(() -> new NewMessage(null, "t", Payload.of(new byte[0])))
        .isInstanceOf(NullPointerException.class);
  }
}
