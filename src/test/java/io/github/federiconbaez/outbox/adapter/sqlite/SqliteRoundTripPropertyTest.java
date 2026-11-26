package io.github.federiconbaez.outbox.adapter.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.federiconbaez.outbox.application.port.UpdateOutcome;
import io.github.federiconbaez.outbox.domain.Lease;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.domain.QuarantineReason;
import io.github.federiconbaez.outbox.domain.RelayId;
import io.github.federiconbaez.outbox.domain.Result;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

/**
 * Ida y vuelta por SQLite sin pérdida: cualquier mensaje (los cuatro estados, ambos motivos de
 * cuarentena, instantes con nanos en todo el rango de años 1..9999, payload binario y texto
 * Unicode) se lee igual a como se escribió.
 */
class SqliteRoundTripPropertyTest {
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final AtomicLong IDS = new AtomicLong();
  private static Path dir;
  private static SqliteOutboxDatabase db;

  @BeforeContainer
  static void open() throws IOException {
    dir = Files.createTempDirectory("outbox-roundtrip");
    db = SqliteOutboxDatabase.open(dir.resolve("rt.db"));
  }

  @AfterContainer
  static void close() throws IOException {
    db.close();
    try (var files = Files.list(dir)) {
      for (Path p : files.toList()) {
        Files.delete(p);
      }
    }
    Files.delete(dir);
  }

  @Property(tries = 300)
  void anyMessageSurvivesARoundTrip(@ForAll("messages") OutboxMessage generated) {
    MessageId id = new MessageId("rt-" + IDS.incrementAndGet());
    OutboxMessage expected =
        new OutboxMessage(
            id,
            generated.aggregateId(),
            generated.type(),
            generated.payload(),
            generated.attempts(),
            generated.status(),
            generated.nextAttemptAt(),
            generated.createdAt());
    db.inTransaction(
        tx -> {
          tx.appendOutbox(OutboxMessage.pending(id, "agg", "T", Payload.of(new byte[0]), T0));
          return null;
        });
    UpdateOutcome outcome = db.update(id, current -> Result.ok(expected));
    assertThat(outcome).isEqualTo(new UpdateOutcome.Updated(expected));
    assertThat(db.find(id)).contains(expected);
  }

  @Provide
  Arbitrary<OutboxMessage> messages() {
    Arbitrary<String> text =
        Arbitraries.strings()
            .withCharRange('\u0001', '\uD7FF')
            .withCharRange('\uE000', '\uFFFD')
            .ofMinLength(1)
            .ofMaxLength(40)
            .filter(s -> !s.isBlank());
    Arbitrary<Instant> instants =
        Combinators.combine(
                Arbitraries.longs().between(-62_135_596_800L, 253_402_300_799L),
                Arbitraries.integers().between(0, 999_999_999))
            .as(Instant::ofEpochSecond);
    Arbitrary<RelayId> relays = text.map(RelayId::new);
    Arbitrary<OutboxStatus> statuses =
        Arbitraries.oneOf(
            Arbitraries.just(new OutboxStatus.Pending()),
            Combinators.combine(
                    relays, Arbitraries.integers().between(1, Integer.MAX_VALUE), instants)
                .as((o, t, e) -> new OutboxStatus.InFlight(new Lease(o, t, e))),
            Combinators.combine(instants, relays).as(OutboxStatus.Published::new),
            Combinators.combine(
                    Arbitraries.oneOf(
                        Combinators.combine(Arbitraries.integers().between(0, 1_000), text)
                            .as(QuarantineReason.MaxAttemptsExceeded::new),
                        text.map(QuarantineReason.PermanentFailure::new)),
                    instants)
                .as(OutboxStatus.Quarantined::new));
    return Combinators.combine(
            text,
            text,
            Arbitraries.bytes().array(byte[].class).ofMaxSize(64).map(Payload::of),
            Arbitraries.integers().between(0, Integer.MAX_VALUE),
            statuses,
            instants,
            instants)
        .as(
            (agg, type, payload, attempts, status, next, created) ->
                new OutboxMessage(
                    new MessageId("ignored"), agg, type, payload, attempts, status, next, created));
  }
}
