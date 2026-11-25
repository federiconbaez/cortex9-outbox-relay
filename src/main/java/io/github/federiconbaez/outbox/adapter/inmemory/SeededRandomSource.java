package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.application.port.RandomSource;
import java.util.SplittableRandom;

/** Azar reproducible: misma semilla, misma secuencia. */
public final class SeededRandomSource implements RandomSource {
  private final SplittableRandom random;

  public SeededRandomSource(long seed) {
    this.random = new SplittableRandom(seed);
  }

  @Override
  public synchronized long nextLong(long boundExclusive) {
    return random.nextLong(boundExclusive);
  }
}
