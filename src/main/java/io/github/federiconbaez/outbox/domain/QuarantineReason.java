package io.github.federiconbaez.outbox.domain;

import java.util.Objects;

/** Por qué un mensaje dejó de reintentarse. */
public sealed interface QuarantineReason {

  record MaxAttemptsExceeded(int attempts, String lastError) implements QuarantineReason {
    public MaxAttemptsExceeded {
      Objects.requireNonNull(lastError, "lastError");
    }
  }

  record PermanentFailure(String reason) implements QuarantineReason {
    public PermanentFailure {
      Objects.requireNonNull(reason, "reason");
    }
  }
}
