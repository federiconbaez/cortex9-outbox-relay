package io.github.federiconbaez.outbox.adapter.sqlite;

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
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Outbox y tabla de negocio en un archivo SQLite. Cumple la suite de contrato {@code
 * OutboxStoreContract} igual que el adapter en memoria. Las reglas siguen en el dominio: el adapter
 * solo lee la fila, llama a la transición pura y escribe el resultado dentro de la misma
 * transacción de escritura.
 */
public final class SqliteOutboxDatabase implements UnitOfWork, OutboxStore, AutoCloseable {
  private static final Duration DEFAULT_BUSY_TIMEOUT = Duration.ofSeconds(10);

  private final SqliteSession session;

  private SqliteOutboxDatabase(SqliteSession session) {
    this.session = session;
  }

  /** Abre (o crea) el archivo y aplica el esquema. */
  public static SqliteOutboxDatabase open(Path file) {
    return new SqliteOutboxDatabase(SqliteSession.open(file, DEFAULT_BUSY_TIMEOUT));
  }

  @Override
  public <T> T inTransaction(Function<Transaction, T> work) {
    Objects.requireNonNull(work, "work");
    Buffer buffer = new Buffer();
    T result = work.apply(buffer);
    session.inTransaction(
        c -> {
          for (Map.Entry<String, String> e : buffer.records.entrySet()) {
            try (PreparedStatement ps =
                c.prepareStatement(
                    "INSERT INTO business_record (key, value) VALUES (?, ?)"
                        + " ON CONFLICT (key) DO UPDATE SET value = excluded.value")) {
              ps.setString(1, e.getKey());
              ps.setString(2, e.getValue());
              ps.executeUpdate();
            }
          }
          for (OutboxMessage m : buffer.messages) {
            if (load(c, m.id()) != null) {
              throw new IllegalStateException("duplicate message id " + m.id());
            }
            try (PreparedStatement ps =
                c.prepareStatement(
                    "INSERT INTO outbox ("
                        + OutboxRows.COLUMNS
                        + ") VALUES ("
                        + OutboxRows.PLACEHOLDERS
                        + ")")) {
              ps.setString(1, m.id().value());
              OutboxRows.bindBody(ps, 2, m);
              ps.executeUpdate();
            }
          }
          return Boolean.TRUE;
        });
    return result;
  }

  @Override
  public List<OutboxMessage> claimDue(RelayId owner, Instant now, Duration lease, int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be >= 1");
    }
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(now, "now");
    Objects.requireNonNull(lease, "lease");
    return session.inTransaction(
        c -> {
          List<OutboxMessage> due = new ArrayList<>();
          try (PreparedStatement ps =
              c.prepareStatement(
                  "SELECT * FROM outbox WHERE status = 'PENDING'"
                      + " AND (next_attempt_s < ? OR (next_attempt_s = ? AND next_attempt_n <= ?))"
                      + " ORDER BY next_attempt_s, next_attempt_n, seq LIMIT ?")) {
            ps.setLong(1, now.getEpochSecond());
            ps.setLong(2, now.getEpochSecond());
            ps.setInt(3, now.getNano());
            ps.setInt(4, limit);
            try (ResultSet rs = ps.executeQuery()) {
              while (rs.next()) {
                due.add(OutboxRows.read(rs));
              }
            }
          }
          List<OutboxMessage> claimed = new ArrayList<>(due.size());
          for (OutboxMessage m : due) {
            if (m.claim(owner, now, lease) instanceof Result.Ok<OutboxMessage> ok) {
              save(c, ok.value());
              claimed.add(ok.value());
            }
          }
          return List.copyOf(claimed);
        });
  }

  @Override
  public UpdateOutcome update(MessageId id, Transition transition) {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(transition, "transition");
    return session.inTransaction(
        c -> {
          OutboxMessage current = load(c, id);
          if (current == null) {
            return new UpdateOutcome.NotFound(id);
          }
          return switch (transition.apply(current)) {
            case Result.Ok<OutboxMessage> ok -> {
              if (!ok.value().id().equals(id)) {
                throw new IllegalStateException("transition changed the message id");
              }
              save(c, ok.value());
              yield new UpdateOutcome.Updated(ok.value());
            }
            case Result.Err<OutboxMessage> err -> new UpdateOutcome.Rejected(err.error());
          };
        });
  }

  @Override
  public List<MessageId> expiredLeases(Instant now) {
    Objects.requireNonNull(now, "now");
    return query("SELECT * FROM outbox WHERE status = 'IN_FLIGHT' ORDER BY seq").stream()
        .filter(m -> m.status() instanceof OutboxStatus.InFlight f && !f.lease().isValidAt(now))
        .map(OutboxMessage::id)
        .toList();
  }

  @Override
  public Optional<OutboxMessage> find(MessageId id) {
    Objects.requireNonNull(id, "id");
    return session.inTransaction(c -> Optional.ofNullable(load(c, id)));
  }

  @Override
  public List<OutboxMessage> snapshot() {
    return query("SELECT * FROM outbox ORDER BY seq");
  }

  /** Copia de la tabla de negocio confirmada. */
  public Map<String, String> records() {
    return session.inTransaction(
        c -> {
          Map<String, String> out = new LinkedHashMap<>();
          try (PreparedStatement ps =
                  c.prepareStatement("SELECT key, value FROM business_record ORDER BY key");
              ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              out.put(rs.getString(1), rs.getString(2));
            }
          }
          return Map.copyOf(out);
        });
  }

  private List<OutboxMessage> query(String sql) {
    return session.inTransaction(
        c -> {
          List<OutboxMessage> out = new ArrayList<>();
          try (PreparedStatement ps = c.prepareStatement(sql);
              ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              out.add(OutboxRows.read(rs));
            }
          }
          return List.copyOf(out);
        });
  }

  private static @org.jspecify.annotations.Nullable OutboxMessage load(Connection c, MessageId id)
      throws SQLException {
    try (PreparedStatement ps = c.prepareStatement("SELECT * FROM outbox WHERE id = ?")) {
      ps.setString(1, id.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? OutboxRows.read(rs) : null;
      }
    }
  }

  private static void save(Connection c, OutboxMessage m) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("UPDATE outbox SET " + OutboxRows.ASSIGNMENTS + " WHERE id = ?")) {
      int next = OutboxRows.bindBody(ps, 1, m);
      ps.setString(next, m.id().value());
      if (ps.executeUpdate() != 1) {
        throw new StorageException("message vanished during update: " + m.id());
      }
    }
  }

  @Override
  public void close() {
    session.close();
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
