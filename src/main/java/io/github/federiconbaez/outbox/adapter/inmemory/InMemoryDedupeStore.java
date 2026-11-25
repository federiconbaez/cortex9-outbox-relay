package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.application.port.DedupeStore;
import io.github.federiconbaez.outbox.domain.MessageId;
import java.util.HashSet;
import java.util.Set;

/** Dedupe en memoria. El lock simula la transacción local del consumidor (ADR-0002). */
public final class InMemoryDedupeStore implements DedupeStore {
  private final Set<MessageId> processed = new HashSet<>();

  @Override
  public synchronized boolean runOnce(MessageId id, Runnable effect) {
    if (processed.contains(id)) {
      return false;
    }
    effect.run();
    processed.add(id);
    return true;
  }

  @Override
  public synchronized boolean seen(MessageId id) {
    return processed.contains(id);
  }
}
