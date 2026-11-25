package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.Payload;

/** Lo que viaja al broker. No expone el lease ni el estado interno del outbox. */
public record Envelope(
    MessageId messageId, String aggregateId, String type, Payload payload, int attempt) {

  public static Envelope of(OutboxMessage message) {
    return new Envelope(
        message.id(), message.aggregateId(), message.type(), message.payload(), message.attempts());
  }
}
