package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.application.port.Clock;
import io.github.federiconbaez.outbox.application.port.IdGenerator;
import io.github.federiconbaez.outbox.application.port.UnitOfWork;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Escribe el cambio de negocio y sus mensajes en la misma transacción. */
public final class OutboxWriter {
  private final UnitOfWork unitOfWork;
  private final Clock clock;
  private final IdGenerator ids;

  public OutboxWriter(UnitOfWork unitOfWork, Clock clock, IdGenerator ids) {
    this.unitOfWork = Objects.requireNonNull(unitOfWork);
    this.clock = Objects.requireNonNull(clock);
    this.ids = Objects.requireNonNull(ids);
  }

  public List<MessageId> commit(BusinessChange change, List<NewMessage> messages) {
    Objects.requireNonNull(change, "change");
    List<NewMessage> copy = List.copyOf(messages);
    return unitOfWork.inTransaction(
        tx -> {
          change.apply(tx);
          Instant now = clock.now();
          List<MessageId> written = new ArrayList<>(copy.size());
          for (NewMessage m : copy) {
            OutboxMessage message =
                OutboxMessage.pending(ids.next(), m.aggregateId(), m.type(), m.payload(), now);
            tx.appendOutbox(message);
            written.add(message.id());
          }
          return List.copyOf(written);
        });
  }
}
