package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.RelayId;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Tabla de outbox. Ver la suite de contrato {@code OutboxStoreContract} en los tests. */
public interface OutboxStore {

  /**
   * Reclama de forma atómica hasta {@code limit} mensajes confirmados y vencidos (Pending con
   * nextAttemptAt &lt;= now), en orden de nextAttemptAt y luego de inserción. Dos llamadas
   * concurrentes nunca devuelven el mismo mensaje.
   */
  List<OutboxMessage> claimDue(RelayId owner, Instant now, Duration lease, int limit);

  /** Lee, aplica {@code transition} y guarda, todo de forma atómica para ese mensaje. */
  UpdateOutcome update(MessageId id, Transition transition);

  /** Mensajes InFlight cuyo lease venció en {@code now}. */
  List<MessageId> expiredLeases(Instant now);

  Optional<OutboxMessage> find(MessageId id);

  /** Copia de todos los mensajes confirmados, en orden de inserción. */
  List<OutboxMessage> snapshot();
}
