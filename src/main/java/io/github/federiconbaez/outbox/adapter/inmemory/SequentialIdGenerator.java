package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.application.port.IdGenerator;
import io.github.federiconbaez.outbox.domain.MessageId;
import java.util.concurrent.atomic.AtomicLong;

public final class SequentialIdGenerator implements IdGenerator {
  private final String prefix;
  private final AtomicLong next = new AtomicLong(1);

  public SequentialIdGenerator(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public MessageId next() {
    return new MessageId(prefix + "-" + next.getAndIncrement());
  }
}
