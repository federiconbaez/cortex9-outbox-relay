package io.github.federiconbaez.outbox.adapter.fake;

import io.github.federiconbaez.outbox.application.port.Envelope;
import io.github.federiconbaez.outbox.application.port.PublishResult;
import io.github.federiconbaez.outbox.application.port.Publisher;
import io.github.federiconbaez.outbox.domain.MessageId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Broker falso: sigue un guion de pasos por mensaje (luego ACK) y entrega al consumidor. Registra
 * cada entrega para verificar duplicados.
 */
public final class ScriptedPublisher implements Publisher {
  private final Consumer<Envelope> downstream;
  private final Map<MessageId, Deque<Step>> scripts = new HashMap<>();
  private final List<Envelope> delivered = new ArrayList<>();
  private int calls;

  public ScriptedPublisher(Consumer<Envelope> downstream) {
    this.downstream = Objects.requireNonNull(downstream);
  }

  public synchronized ScriptedPublisher script(MessageId id, Step... steps) {
    scripts.computeIfAbsent(id, k -> new ArrayDeque<>()).addAll(List.of(steps));
    return this;
  }

  @Override
  public PublishResult publish(Envelope envelope) {
    Step step;
    synchronized (this) {
      calls++;
      Deque<Step> script = scripts.get(envelope.messageId());
      Step next = script == null ? null : script.poll();
      step = next == null ? Step.ACK : next;
    }
    return switch (step) {
      case ACK -> {
        deliver(envelope);
        yield new PublishResult.Ack();
      }
      case TRANSIENT -> new PublishResult.Transient("scripted transient");
      case PERMANENT -> new PublishResult.Permanent("scripted permanent");
      case DELIVER_THEN_TRANSIENT -> {
        deliver(envelope);
        yield new PublishResult.Transient("ack lost");
      }
      case THROW -> throw new IllegalStateException("scripted broker client failure");
    };
  }

  private void deliver(Envelope envelope) {
    synchronized (this) {
      delivered.add(envelope);
    }
    downstream.accept(envelope);
  }

  public synchronized List<Envelope> delivered() {
    return List.copyOf(delivered);
  }

  public synchronized int calls() {
    return calls;
  }
}
