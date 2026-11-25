package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.Result;

/** Transición pura que el store aplica de forma atómica sobre el valor actual. */
@FunctionalInterface
public interface Transition {
  Result<OutboxMessage> apply(OutboxMessage current);
}
