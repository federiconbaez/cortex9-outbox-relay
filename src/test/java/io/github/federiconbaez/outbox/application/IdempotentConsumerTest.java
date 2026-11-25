package io.github.federiconbaez.outbox.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.adapter.inmemory.InMemoryDedupeStore;
import io.github.federiconbaez.outbox.application.port.Envelope;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.Payload;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IdempotentConsumerTest {
  private final InMemoryDedupeStore dedupe = new InMemoryDedupeStore();
  private final AtomicInteger effects = new AtomicInteger();

  private static Envelope envelope(String id) {
    return new Envelope(new MessageId(id), "order-1", "OrderPlaced", Payload.of(new byte[0]), 1);
  }

  @Test
  void appliesOnceAndReportsDuplicates() {
    IdempotentConsumer consumer = new IdempotentConsumer(dedupe, e -> effects.incrementAndGet());
    assertThat(consumer.handle(envelope("m-1"))).isEqualTo(ConsumeResult.APPLIED);
    assertThat(consumer.handle(envelope("m-1"))).isEqualTo(ConsumeResult.DUPLICATE);
    consumer.accept(envelope("m-2"));
    assertThat(effects).hasValue(2);
    assertThat(dedupe.seen(new MessageId("m-1"))).isTrue();
  }

  @Test
  void failedEffectIsNotRecordedSoRedeliveryRetriesIt() {
    AtomicInteger calls = new AtomicInteger();
    IdempotentConsumer consumer =
        new IdempotentConsumer(
            dedupe,
            e -> {
              if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("downstream down");
              }
            });
    assertThatThrownBy(() -> consumer.handle(envelope("m-1")))
        .isInstanceOf(IllegalStateException.class);
    assertThat(dedupe.seen(new MessageId("m-1"))).isFalse();
    assertThat(consumer.handle(envelope("m-1"))).isEqualTo(ConsumeResult.APPLIED);
    assertThat(calls).hasValue(2);
  }
}
