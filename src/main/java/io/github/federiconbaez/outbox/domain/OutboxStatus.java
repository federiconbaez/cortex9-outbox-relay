package io.github.federiconbaez.outbox.domain;

import java.time.Instant;
import java.util.Objects;

/** Ciclo de vida de un mensaje: Pending → InFlight → (Published | Pending | Quarantined). */
public sealed interface OutboxStatus {

  boolean isTerminal();

  record Pending() implements OutboxStatus {
    @Override
    public boolean isTerminal() {
      return false;
    }
  }

  record InFlight(Lease lease) implements OutboxStatus {
    public InFlight {
      Objects.requireNonNull(lease, "lease");
    }

    @Override
    public boolean isTerminal() {
      return false;
    }
  }

  record Published(Instant at, RelayId by) implements OutboxStatus {
    public Published {
      Objects.requireNonNull(at, "at");
      Objects.requireNonNull(by, "by");
    }

    @Override
    public boolean isTerminal() {
      return true;
    }
  }

  record Quarantined(QuarantineReason reason, Instant at) implements OutboxStatus {
    public Quarantined {
      Objects.requireNonNull(reason, "reason");
      Objects.requireNonNull(at, "at");
    }

    @Override
    public boolean isTerminal() {
      return true;
    }
  }
}
