package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.domain.OutboxMessage;
import java.util.List;

/** Resultado de un ciclo de reclamo: leases vencidos liberados y mensajes reclamados. */
public record Claimed(int released, List<OutboxMessage> messages) {
  public Claimed {
    messages = List.copyOf(messages);
  }
}
