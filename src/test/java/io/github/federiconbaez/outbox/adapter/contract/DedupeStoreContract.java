package io.github.federiconbaez.outbox.adapter.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.federiconbaez.outbox.application.port.DedupeStore;
import io.github.federiconbaez.outbox.domain.MessageId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Suite de contrato de {@link DedupeStore}: la corren el fake en memoria y el adapter SQLite. */
public abstract class DedupeStoreContract {
  private static final MessageId M1 = new MessageId("m-1");
  private static final MessageId M2 = new MessageId("m-2");

  protected abstract DedupeStore create();

  private DedupeStore store;

  @BeforeEach
  void setUp() {
    store = create();
  }

  @Test
  void runsTheEffectOncePerId() {
    AtomicInteger effects = new AtomicInteger();
    assertThat(store.seen(M1)).isFalse();
    assertThat(store.runOnce(M1, effects::incrementAndGet)).isTrue();
    assertThat(store.runOnce(M1, effects::incrementAndGet)).isFalse();
    assertThat(store.seen(M1)).isTrue();
    assertThat(store.seen(M2)).isFalse();
    assertThat(store.runOnce(M2, effects::incrementAndGet)).isTrue();
    assertThat(effects).hasValue(2);
  }

  @Test
  void failedEffectIsNotRecordedAndCanBeRetried() {
    assertThatThrownBy(
            () ->
                store.runOnce(
                    M1,
                    () -> {
                      throw new IllegalStateException("downstream failed");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("downstream failed");
    assertThat(store.seen(M1)).isFalse();
    AtomicInteger effects = new AtomicInteger();
    assertThat(store.runOnce(M1, effects::incrementAndGet)).isTrue();
    assertThat(effects).hasValue(1);
  }

  @Test
  void concurrentDeliveriesOfTheSameIdRunTheEffectOnce() throws Exception {
    int threads = 8;
    AtomicInteger effects = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Boolean>> results = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      results.add(
          pool.submit(
              () -> {
                start.await();
                return store.runOnce(M1, effects::incrementAndGet);
              }));
    }
    start.countDown();
    int ran = 0;
    for (Future<Boolean> f : results) {
      if (f.get(30, TimeUnit.SECONDS)) {
        ran++;
      }
    }
    pool.shutdown();
    assertThat(ran).isEqualTo(1);
    assertThat(effects).hasValue(1);
  }
}
