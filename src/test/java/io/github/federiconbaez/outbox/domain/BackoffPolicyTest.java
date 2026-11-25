package io.github.federiconbaez.outbox.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {
  private final BackoffPolicy policy =
      new BackoffPolicy(Duration.ofMillis(100), Duration.ofMillis(1000), 5);

  @Test
  void ceilingDoublesFromBaseUntilCap() {
    assertThat(policy.ceilingMillis(1)).isEqualTo(100);
    assertThat(policy.ceilingMillis(2)).isEqualTo(200);
    assertThat(policy.ceilingMillis(3)).isEqualTo(400);
    assertThat(policy.ceilingMillis(4)).isEqualTo(800);
    assertThat(policy.ceilingMillis(5)).isEqualTo(1000);
    assertThat(policy.ceilingMillis(1_000_000)).isEqualTo(1000);
  }

  @Test
  void ceilingSaturatesWithoutOverflowForHugeCaps() {
    BackoffPolicy huge =
        new BackoffPolicy(Duration.ofMillis(3), Duration.ofMillis(Long.MAX_VALUE / 2 + 7), 1);
    assertThat(huge.ceilingMillis(Integer.MAX_VALUE)).isEqualTo(Long.MAX_VALUE / 2 + 7);
    assertThat(huge.ceilingMillis(2)).isEqualTo(6);
    BackoffPolicy max =
        new BackoffPolicy(Duration.ofMillis(3), Duration.ofMillis(Long.MAX_VALUE), 1);
    assertThat(max.ceilingMillis(64)).isEqualTo(Long.MAX_VALUE);
    assertThat(max.ceilingMillis(62)).isEqualTo(3L << 61);
  }

  @Test
  void ceilingEqualToHalfCapDoublesExactlyToCap() {
    BackoffPolicy p = new BackoffPolicy(Duration.ofMillis(50), Duration.ofMillis(100), 1);
    assertThat(p.ceilingMillis(2)).isEqualTo(100);
    BackoffPolicy odd = new BackoffPolicy(Duration.ofMillis(51), Duration.ofMillis(101), 1);
    assertThat(odd.ceilingMillis(2)).isEqualTo(101);
    BackoffPolicy halfOfOdd = new BackoffPolicy(Duration.ofMillis(50), Duration.ofMillis(101), 1);
    assertThat(halfOfOdd.ceilingMillis(2)).isEqualTo(100);
    assertThat(halfOfOdd.ceilingMillis(3)).isEqualTo(101);
  }

  @Test
  void delayIsHalfCeilingPlusJitterDraw() {
    assertThat(policy.jitterBound(1)).isEqualTo(51);
    assertThat(policy.delay(1, 0)).isEqualTo(Duration.ofMillis(50));
    assertThat(policy.delay(1, 50)).isEqualTo(Duration.ofMillis(100));
    assertThat(policy.delay(5, 7)).isEqualTo(Duration.ofMillis(507));
    BackoffPolicy oddCap = new BackoffPolicy(Duration.ofMillis(3), Duration.ofMillis(3), 1);
    assertThat(oddCap.jitterBound(1)).isEqualTo(3);
    assertThat(oddCap.delay(1, 2)).isEqualTo(Duration.ofMillis(3));
  }

  @Test
  void rejectsOutOfRangeInputs() {
    assertThatThrownBy(() -> policy.ceilingMillis(0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> policy.delay(1, -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> policy.delay(1, 51)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void exhaustedAtMaxAttempts() {
    assertThat(policy.exhausted(4)).isFalse();
    assertThat(policy.exhausted(5)).isTrue();
    assertThat(policy.exhausted(6)).isTrue();
  }
}
