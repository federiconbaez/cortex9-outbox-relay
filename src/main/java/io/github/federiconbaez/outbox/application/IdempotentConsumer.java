package io.github.federiconbaez.outbox.application;

import io.github.federiconbaez.outbox.application.port.DedupeStore;
import io.github.federiconbaez.outbox.application.port.Envelope;
import java.util.Objects;
import java.util.function.Consumer;

/** Consumidor idempotente: at-least-once + dedupe por MessageId = effectively-once. */
public final class IdempotentConsumer implements Consumer<Envelope> {
  private final DedupeStore dedupe;
  private final Consumer<Envelope> effect;

  public IdempotentConsumer(DedupeStore dedupe, Consumer<Envelope> effect) {
    this.dedupe = Objects.requireNonNull(dedupe);
    this.effect = Objects.requireNonNull(effect);
  }

  public ConsumeResult handle(Envelope envelope) {
    boolean applied = dedupe.runOnce(envelope.messageId(), () -> effect.accept(envelope));
    return applied ? ConsumeResult.APPLIED : ConsumeResult.DUPLICATE;
  }

  @Override
  public void accept(Envelope envelope) {
    handle(envelope);
  }
}
