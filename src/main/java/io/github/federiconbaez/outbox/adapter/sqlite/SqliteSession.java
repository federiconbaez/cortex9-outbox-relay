package io.github.federiconbaez.outbox.adapter.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;

/**
 * Una conexión JDBC a un archivo SQLite con transacciones {@code BEGIN IMMEDIATE}. Dentro del
 * proceso serializa con un lock; entre procesos manda el lock de escritura del archivo (con
 * busy_timeout para esperar en vez de fallar).
 */
final class SqliteSession implements AutoCloseable {
  static final int SCHEMA_VERSION = 1;

  @FunctionalInterface
  interface Work<T> {
    T run(Connection connection) throws SQLException;
  }

  private final Object lock = new Object();
  private final Connection connection;

  private SqliteSession(Connection connection) {
    this.connection = connection;
  }

  static SqliteSession open(Path file, Duration busyTimeout) {
    Objects.requireNonNull(file, "file");
    try {
      Connection c = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
      try (Statement s = c.createStatement()) {
        s.execute("PRAGMA busy_timeout = " + busyTimeout.toMillis());
        s.execute("PRAGMA journal_mode = WAL");
        s.execute("PRAGMA synchronous = FULL");
      }
      SqliteSession session = new SqliteSession(c);
      session.migrate();
      return session;
    } catch (SQLException e) {
      throw new StorageException("cannot open " + file, e);
    }
  }

  private void migrate() {
    inTransaction(
        c -> {
          try (Statement s = c.createStatement()) {
            int version;
            try (var rs = s.executeQuery("PRAGMA user_version")) {
              version = rs.next() ? rs.getInt(1) : 0;
            }
            if (version > SCHEMA_VERSION) {
              throw new StorageException(
                  "schema version " + version + " is newer than supported " + SCHEMA_VERSION);
            }
            s.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  id TEXT NOT NULL UNIQUE,
                  aggregate_id TEXT NOT NULL,
                  type TEXT NOT NULL,
                  payload BLOB NOT NULL,
                  attempts INTEGER NOT NULL CHECK (attempts >= 0),
                  status TEXT NOT NULL
                    CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED', 'QUARANTINED')),
                  next_attempt_s INTEGER NOT NULL,
                  next_attempt_n INTEGER NOT NULL,
                  created_at TEXT NOT NULL,
                  lease_owner TEXT,
                  lease_token INTEGER,
                  lease_expires_at TEXT,
                  published_at TEXT,
                  published_by TEXT,
                  quarantine_kind TEXT CHECK (quarantine_kind IN ('MAX_ATTEMPTS', 'PERMANENT')),
                  quarantine_attempts INTEGER,
                  quarantine_text TEXT,
                  quarantined_at TEXT
                )""");
            s.execute(
                "CREATE INDEX IF NOT EXISTS outbox_due"
                    + " ON outbox (status, next_attempt_s, next_attempt_n, seq)");
            s.execute(
                "CREATE TABLE IF NOT EXISTS business_record"
                    + " (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS processed_message (id TEXT PRIMARY KEY)");
            s.execute("PRAGMA user_version = " + SCHEMA_VERSION);
          }
          return Boolean.TRUE;
        });
  }

  /** Ejecuta {@code work} en una transacción de escritura; si lanza, hace rollback y relanza. */
  <T> T inTransaction(Work<T> work) {
    synchronized (lock) {
      try {
        execute("BEGIN IMMEDIATE");
      } catch (SQLException e) {
        throw new StorageException("cannot begin transaction", e);
      }
      T result;
      try {
        result = work.run(connection);
      } catch (SQLException e) {
        rollbackQuietly(e);
        throw new StorageException("transaction failed", e);
      } catch (RuntimeException | Error e) {
        rollbackQuietly(e);
        throw e;
      }
      try {
        execute("COMMIT");
      } catch (SQLException e) {
        rollbackQuietly(e);
        throw new StorageException("commit failed", e);
      }
      return result;
    }
  }

  private void execute(String sql) throws SQLException {
    try (Statement s = connection.createStatement()) {
      s.execute(sql);
    }
  }

  private void rollbackQuietly(Throwable original) {
    try {
      execute("ROLLBACK");
    } catch (SQLException e) {
      original.addSuppressed(e);
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new StorageException("cannot close", e);
      }
    }
  }
}
