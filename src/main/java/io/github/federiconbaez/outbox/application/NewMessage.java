package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.domain.Payload;
import java.util.Objects;

public record NewMessage(String aggregateId, String type, Payload payload) {
  public NewMessage {
    Objects.requireNonNull(aggregateId, "aggregateId");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(payload, "payload");
  }
}
