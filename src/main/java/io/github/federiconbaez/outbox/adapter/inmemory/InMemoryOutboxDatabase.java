package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.application.port.OutboxStore;
import io.github.federiconbaez.outbox.application.port.Transaction;
import io.github.federiconbaez.outbox.application.port.Transition;
import io.github.federiconbaez.outbox.application.port.UnitOfWork;
import io.github.federiconbaez.outbox.application.port.UpdateOutcome;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.RelayId;
import io.github.federiconbaez.outbox.domain.Result;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * "Base de datos" en memoria: una tabla de negocio clave/valor y la tabla de outbox, con
 * transacciones atómicas (un lock global) e inyección de crashes para tests.
 */
public final class InMemoryOutboxDatabase implements UnitOfWork, OutboxStore {
  private final Object lock = new Object();
  private final Map<String, String> records = new HashMap<>();
  private final LinkedHashMap<MessageId, OutboxMessage> outbox = new LinkedHashMap<>();
  private final Map<MessageId, Long> insertionOrder = new HashMap<>();
  private long sequence;
  private int crashBeforeCommits;
  private int crashBeforeUpdates;

  /** La próxima transacción ejecuta su trabajo pero el proceso "muere" antes del commit. */
  public void crashBeforeNextCommit() {
    synchronized (lock) {
      crashBeforeCommits++;
    }
  }

  /** La próxima llamada a update "muere" antes de escribir (crash entre publicar y marcar). */
  public void crashBeforeNextUpdate() {
    synchronized (lock) {
      crashBeforeUpdates++;
    }
  }

  @Override
  public <T> T inTransaction(Function<Transaction, T> work) {
    Objects.requireNonNull(work, "work");
    synchronized (lock) {
      Buffer buffer = new Buffer();
      T result = work.apply(buffer);
      if (crashBeforeCommits > 0) {
        crashBeforeCommits--;
        throw new SimulatedCrash("before commit");
      }
      for (OutboxMessage m : buffer.messages) {
        if (outbox.containsKey(m.id())) {
          throw new IllegalStateException("duplicate message id " + m.id());
        }
      }
      records.putAll(buffer.records);
      for (OutboxMessage m : buffer.messages) {
        outbox.put(m.id(), m);
        insertionOrder.put(m.id(), sequence++);
      }
      return result;
    }
  }

  @Override
  public List<OutboxMessage> claimDue(RelayId owner, Instant now, Duration lease, int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be >= 1");
    }
    synchronized (lock) {
      List<OutboxMessage> due =
          outbox.values().stream()
              .filter(m -> m.isDue(now))
              .sorted(
                  Comparator.comparing(OutboxMessage::nextAttemptAt)
                      .thenComparing(m -> insertionOrder.getOrDefault(m.id(), 0L)))
              .limit(limit)
              .toList();
      List<OutboxMessage> claimed = new ArrayList<>(due.size());
      for (OutboxMessage m : due) {
        if (m.claim(owner, now, lease) instanceof Result.Ok<OutboxMessage> ok) {
          outbox.put(m.id(), ok.value());
          claimed.add(ok.value());
        }
      }
      return List.copyOf(claimed);
    }
  }

  @Override
  public UpdateOutcome update(MessageId id, Transition transition) {
    synchronized (lock) {
      if (crashBeforeUpdates > 0) {
        crashBeforeUpdates--;
        throw new SimulatedCrash("before update of " + id);
      }
      OutboxMessage current = outbox.get(id);
      if (current == null) {
        return new UpdateOutcome.NotFound(id);
      }
      return switch (transition.apply(current)) {
        case Result.Ok<OutboxMessage> ok -> {
          outbox.put(id, ok.value());
          yield new UpdateOutcome.Updated(ok.value());
        }
        case Result.Err<OutboxMessage> err -> new UpdateOutcome.Rejected(err.error());
      };
    }
  }

  @Override
  public List<MessageId> expiredLeases(Instant now) {
    synchronized (lock) {
      return outbox.values().stream()
          .filter(m -> m.status() instanceof OutboxStatus.InFlight f && !f.lease().isValidAt(now))
          .map(OutboxMessage::id)
          .toList();
    }
  }

  @Override
  public Optional<OutboxMessage> find(MessageId id) {
    synchronized (lock) {
      return Optional.ofNullable(outbox.get(id));
    }
  }

  @Override
  public List<OutboxMessage> snapshot() {
    synchronized (lock) {
      return List.copyOf(outbox.values());
    }
  }

  /** Copia de la tabla de negocio confirmada. */
  public Map<String, String> records() {
    synchronized (lock) {
      return Map.copyOf(records);
    }
  }

  private static final class Buffer implements Transaction {
    private final Map<String, String> records = new HashMap<>();
    private final List<OutboxMessage> messages = new ArrayList<>();

    @Override
    public void putRecord(String key, String value) {
      records.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
    }

    @Override
    public void appendOutbox(OutboxMessage message) {
      Objects.requireNonNull(message, "message");
      if (!(message.status() instanceof OutboxStatus.Pending)) {
        throw new IllegalArgumentException("only pending messages can be appended");
      }
      messages.add(message);
    }
  }
}
