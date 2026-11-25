package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.DomainError;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;

public sealed interface UpdateOutcome {
  record Updated(OutboxMessage message) implements UpdateOutcome {}

  record Rejected(DomainError error) implements UpdateOutcome {}

  record NotFound(MessageId id) implements UpdateOutcome {}
}
