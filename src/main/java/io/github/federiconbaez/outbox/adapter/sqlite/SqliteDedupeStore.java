package io.github.federiconbaez.outbox.adapter.sqlite;

import io.github.federiconbaez.outbox.application.port.DedupeStore;
import io.github.federiconbaez.outbox.domain.MessageId;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Objects;

/**
 * Dedupe durable. {@code runOnce} toma el lock de escritura ({@code BEGIN IMMEDIATE}), mira si el
 * id ya está, corre el efecto y registra el id en la misma transacción. Si el efecto lanza,
 * rollback y el id queda libre para reintentar.
 *
 * <p>Límite honesto: el efecto corre dentro de la transacción pero no forma parte de ella. Si es un
 * efecto externo (HTTP, otro sistema) y el proceso muere después del efecto y antes del commit, el
 * efecto se repite al reintentar. Effectively-once real exige que el efecto escriba en esta misma
 * base (ADR-0002).
 */
public final class SqliteDedupeStore implements DedupeStore, AutoCloseable {
  private final SqliteSession session;

  private SqliteDedupeStore(SqliteSession session) {
    this.session = session;
  }

  public static SqliteDedupeStore open(Path file) {
    return new SqliteDedupeStore(SqliteSession.open(file, Duration.ofSeconds(10)));
  }

  @Override
  public boolean runOnce(MessageId id, Runnable effect) {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(effect, "effect");
    return session.inTransaction(
        c -> {
          if (exists(c, id)) {
            return false;
          }
          effect.run();
          try (PreparedStatement ps =
              c.prepareStatement("INSERT INTO processed_message (id) VALUES (?)")) {
            ps.setString(1, id.value());
            ps.executeUpdate();
          }
          return true;
        });
  }

  @Override
  public boolean seen(MessageId id) {
    Objects.requireNonNull(id, "id");
    return session.inTransaction(c -> exists(c, id));
  }

  private static boolean exists(java.sql.Connection c, MessageId id) throws java.sql.SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("SELECT 1 FROM processed_message WHERE id = ?")) {
      ps.setString(1, id.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  @Override
  public void close() {
    session.close();
  }
}
