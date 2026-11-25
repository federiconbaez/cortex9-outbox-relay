package io.github.federiconbaez.outbox.domain;

import static io.github.federiconbaez.outbox.domain.Fixtures.A;
import static io.github.federiconbaez.outbox.domain.Fixtures.T0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ValueObjectsTest {

  @Test
  void idsRejectBlankAndPrintTheirValue() {
    assertThatThrownBy(() -> new MessageId(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RelayId("")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new MessageId(null)).isInstanceOf(NullPointerException.class);
    assertThat(new MessageId("m-1")).hasToString("m-1");
    assertThat(new RelayId("r")).hasToString("r");
  }

  @Test
  void payloadIsDefensivelyCopiedAndComparedByContent() {
    byte[] raw = {1, 2, 3};
    Payload p = Payload.of(raw);
    raw[0] = 9;
    assertThat(p.bytes()).containsExactly(1, 2, 3);
    p.bytes()[1] = 9;
    assertThat(p.bytes()).containsExactly(1, 2, 3);
    assertThat(p.size()).isEqualTo(3);
    assertThat(p).isEqualTo(Payload.of(new byte[] {1, 2, 3})).isNotEqualTo(Payload.of(new byte[0]));
    assertThat(p).isNotEqualTo("x");
    assertThat(p.hashCode()).isEqualTo(Payload.of(new byte[] {1, 2, 3}).hashCode());
    assertThat(p).hasToString("Payload[3 bytes]");
    assertThat(Payload.of(new byte[] {1}).hashCode())
        .isNotEqualTo(Payload.of(new byte[] {2}).hashCode());
  }

  @Test
  void leaseIsValidStrictlyBeforeExpiry() {
    Lease lease = new Lease(A, 1, T0.plusSeconds(10));
    assertThat(lease.isValidAt(T0.plusSeconds(9))).isTrue();
    assertThat(lease.isValidAt(T0.plusSeconds(10))).isFalse();
    assertThatThrownBy(() -> new Lease(A, 0, T0)).isInstanceOf(IllegalArgumentException.class);
    assertThat(new Lease(A, 1, T0).token()).isEqualTo(1);
  }

  @Test
  void statusesKnowWhetherTheyAreTerminal() {
    assertThat(new OutboxStatus.Pending().isTerminal()).isFalse();
    assertThat(new OutboxStatus.InFlight(new Lease(A, 1, T0)).isTerminal()).isFalse();
    assertThat(new OutboxStatus.Published(T0, A).isTerminal()).isTrue();
    assertThat(
            new OutboxStatus.Quarantined(new QuarantineReason.PermanentFailure("x"), T0)
                .isTerminal())
        .isTrue();
  }

  @Test
  void messageValidatesItsFields() {
    OutboxMessage m = Fixtures.pending("m-1");
    assertThatThrownBy(
            () -> new OutboxMessage(m.id(), " ", m.type(), m.payload(), 0, m.status(), T0, T0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new OutboxMessage(m.id(), m.aggregateId(), "", m.payload(), 0, m.status(), T0, T0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new OutboxMessage(
                    m.id(), m.aggregateId(), m.type(), m.payload(), -1, m.status(), T0, T0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(
            new OutboxMessage(m.id(), m.aggregateId(), m.type(), m.payload(), 0, m.status(), T0, T0)
                .attempts())
        .isZero();
  }

  @Test
  void backoffPolicyRejectsInvalidConfiguration() {
    Duration ms = Duration.ofMillis(1);
    assertThatThrownBy(() -> new BackoffPolicy(Duration.ZERO, ms, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BackoffPolicy(Duration.ofMillis(2), ms, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BackoffPolicy(ms, ms, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(new BackoffPolicy(ms, ms, 1).maxAttempts()).isEqualTo(1);
  }
}
