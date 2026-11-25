package io.github.federiconbaez.outbox.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

class BackoffPropertyTest {

  @Property
  void ceilingIsMonotoneAndCapped(
      @ForAll @LongRange(min = 1, max = 10_000) long base,
      @ForAll @LongRange(min = 0, max = 10_000_000) long extra,
      @ForAll @IntRange(min = 1, max = 200) int attempt) {
    BackoffPolicy p =
        new BackoffPolicy(Duration.ofMillis(base), Duration.ofMillis(base + extra), 1);
    long cap = base + extra;
    assertThat(p.ceilingMillis(attempt)).isBetween(base, cap);
    assertThat(p.ceilingMillis(attempt + 1)).isGreaterThanOrEqualTo(p.ceilingMillis(attempt));
  }

  @Property
  void delayStaysWithinHalfCeilingAndCeiling(
      @ForAll @LongRange(min = 1, max = 10_000) long base,
      @ForAll @LongRange(min = 0, max = 10_000_000) long extra,
      @ForAll @IntRange(min = 1, max = 200) int attempt,
      @ForAll @LongRange(min = 0, max = Long.MAX_VALUE - 1) long rawDraw) {
    BackoffPolicy p =
        new BackoffPolicy(Duration.ofMillis(base), Duration.ofMillis(base + extra), 1);
    long draw = rawDraw % p.jitterBound(attempt);
    long ceiling = p.ceilingMillis(attempt);
    long delay = p.delay(attempt, draw).toMillis();
    assertThat(delay).isBetween(ceiling / 2, ceiling);
    assertThat(delay).isLessThanOrEqualTo(base + extra);
  }

  @Property
  void sameJitterQuantileGivesMonotoneDelays(
      @ForAll @LongRange(min = 1, max = 1_000) long base,
      @ForAll @LongRange(min = 0, max = 100_000) long extra,
      @ForAll @IntRange(min = 1, max = 60) int attempt) {
    BackoffPolicy p =
        new BackoffPolicy(Duration.ofMillis(base), Duration.ofMillis(base + extra), 1);
    Assume.that(p.jitterBound(attempt) > 0);
    long lo = p.delay(attempt, 0).toMillis();
    long loNext = p.delay(attempt + 1, 0).toMillis();
    long hi = p.delay(attempt, p.jitterBound(attempt) - 1).toMillis();
    long hiNext = p.delay(attempt + 1, p.jitterBound(attempt + 1) - 1).toMillis();
    assertThat(loNext).isGreaterThanOrEqualTo(lo);
    assertThat(hiNext).isGreaterThanOrEqualTo(hi);
  }
}
