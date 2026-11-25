package io.github.federiconbaez.outbox.domain;

import java.time.Instant;

/** Rechazos esperables de las transiciones. Son datos, no excepciones. */
public sealed interface DomainError {

  MessageId messageId();

  /** El mensaje está pendiente pero su próximo intento es posterior a {@code now}. */
  record NotDue(MessageId messageId, Instant nextAttemptAt) implements DomainError {}

  /** El estado actual no admite la operación pedida. */
  record NotClaimable(MessageId messageId, OutboxStatus status) implements DomainError {}

  /** Quien marca no tiene el lease vigente (otro relay lo reclamó o ya se liberó). */
  record StaleLease(MessageId messageId, RelayId owner, int token) implements DomainError {}

  /** Se intentó liberar un lease que todavía no venció. */
  record LeaseStillValid(MessageId messageId, Instant expiresAt) implements DomainError {}
}
