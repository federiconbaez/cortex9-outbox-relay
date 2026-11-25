package io.github.federiconbaez.outbox.support;

import io.github.federiconbaez.outbox.adapter.fake.ScriptedPublisher;
import io.github.federiconbaez.outbox.adapter.inmemory.InMemoryDedupeStore;
import io.github.federiconbaez.outbox.adapter.inmemory.InMemoryOutboxDatabase;
import io.github.federiconbaez.outbox.adapter.inmemory.ManualClock;
import io.github.federiconbaez.outbox.adapter.inmemory.SeededRandomSource;
import io.github.federiconbaez.outbox.adapter.inmemory.SequentialIdGenerator;
import io.github.federiconbaez.outbox.application.IdempotentConsumer;
import io.github.federiconbaez.outbox.application.NewMessage;
import io.github.federiconbaez.outbox.application.OutboxWriter;
import io.github.federiconbaez.outbox.application.Relay;
import io.github.federiconbaez.outbox.application.RelayConfig;
import io.github.federiconbaez.outbox.application.port.Envelope;
import io.github.federiconbaez.outbox.application.port.Publisher;
import io.github.federiconbaez.outbox.domain.BackoffPolicy;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.domain.RelayId;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Mundo de prueba armado a mano: base, reloj, azar sembrado, broker falso y consumidor. */
public final class World {
  public static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  public static final Duration LEASE = Duration.ofSeconds(30);

  public final InMemoryOutboxDatabase db = new InMemoryOutboxDatabase();
  public final ManualClock clock = new ManualClock(T0);
  public final SeededRandomSource random;
  public final InMemoryDedupeStore dedupe = new InMemoryDedupeStore();
  public final Map<MessageId, Integer> effects = new HashMap<>();
  public final IdempotentConsumer consumer;
  public final ScriptedPublisher publisher;
  public final OutboxWriter writer;
  public final RelayConfig config;

  public World(long seed, BackoffPolicy backoff, int batchSize) {
    this.random = new SeededRandomSource(seed);
    this.consumer =
        new IdempotentConsumer(
            dedupe,
            e -> {
              synchronized (effects) {
                effects.merge(e.messageId(), 1, Integer::sum);
              }
            });
    this.publisher = new ScriptedPublisher(consumer);
    this.writer = new OutboxWriter(db, clock, new SequentialIdGenerator("msg"));
    this.config = new RelayConfig(batchSize, LEASE, backoff);
  }

  public World() {
    this(42, new BackoffPolicy(Duration.ofMillis(100), Duration.ofSeconds(5), 3), 10);
  }

  public Relay relay(String id) {
    return relay(id, publisher);
  }

  public Relay relay(String id, Publisher via) {
    return new Relay(new RelayId(id), db, via, clock, random, config);
  }

  public List<MessageId> write(int count) {
    List<NewMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.add(
          new NewMessage(
              "order-" + i, "OrderPlaced", Payload.of(("p" + i).getBytes(StandardCharsets.UTF_8))));
    }
    return writer.commit(tx -> tx.putRecord("orders", "batch-" + count), messages);
  }

  public OutboxMessage get(MessageId id) {
    return db.find(id).orElseThrow();
  }

  public int effectCount(MessageId id) {
    synchronized (effects) {
      return effects.getOrDefault(id, 0);
    }
  }

  /** Envuelve un publisher para observar cada llamada con el estado del store en ese instante. */
  public Publisher observing(Function<Envelope, Void> observer) {
    return envelope -> {
      observer.apply(envelope);
      return publisher.publish(envelope);
    };
  }
}
