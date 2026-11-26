package io.github.federiconbaez.outbox.adapter.sqlite;

import io.github.federiconbaez.outbox.domain.Lease;
import io.github.federiconbaez.outbox.domain.MessageId;
import io.github.federiconbaez.outbox.domain.OutboxMessage;
import io.github.federiconbaez.outbox.domain.OutboxStatus;
import io.github.federiconbaez.outbox.domain.Payload;
import io.github.federiconbaez.outbox.domain.QuarantineReason;
import io.github.federiconbaez.outbox.domain.RelayId;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Traducción fila ↔ {@link OutboxMessage}. Sin pérdida: los instantes van como texto ISO-8601
 * (nanosegundos incluidos) y {@code nextAttemptAt} además como (segundos, nanos) para ordenar y
 * filtrar en SQL.
 */
final class OutboxRows {
  static final String COLUMNS =
      "id, aggregate_id, type, payload, attempts, status, next_attempt_s, next_attempt_n,"
          + " created_at, lease_owner, lease_token, lease_expires_at, published_at, published_by,"
          + " quarantine_kind, quarantine_attempts, quarantine_text, quarantined_at";
  static final String PLACEHOLDERS = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";
  static final String ASSIGNMENTS =
      "aggregate_id = ?, type = ?, payload = ?, attempts = ?, status = ?, next_attempt_s = ?,"
          + " next_attempt_n = ?, created_at = ?, lease_owner = ?, lease_token = ?,"
          + " lease_expires_at = ?, published_at = ?, published_by = ?, quarantine_kind = ?,"
          + " quarantine_attempts = ?, quarantine_text = ?, quarantined_at = ?";

  private OutboxRows() {}

  /** Liga todas las columnas menos {@code id} a partir del índice {@code from}. */
  static int bindBody(PreparedStatement ps, int from, OutboxMessage m) throws SQLException {
    int i = from;
    ps.setString(i++, m.aggregateId());
    ps.setString(i++, m.type());
    ps.setBytes(i++, m.payload().bytes());
    ps.setInt(i++, m.attempts());
    ps.setString(i++, statusName(m.status()));
    ps.setLong(i++, m.nextAttemptAt().getEpochSecond());
    ps.setInt(i++, m.nextAttemptAt().getNano());
    ps.setString(i++, m.createdAt().toString());
    @Nullable String leaseOwner = null;
    @Nullable Integer leaseToken = null;
    @Nullable String leaseExpires = null;
    @Nullable String publishedAt = null;
    @Nullable String publishedBy = null;
    @Nullable String quarantineKind = null;
    @Nullable Integer quarantineAttempts = null;
    @Nullable String quarantineText = null;
    @Nullable String quarantinedAt = null;
    switch (m.status()) {
      case OutboxStatus.Pending p -> {}
      case OutboxStatus.InFlight f -> {
        leaseOwner = f.lease().owner().value();
        leaseToken = f.lease().token();
        leaseExpires = f.lease().expiresAt().toString();
      }
      case OutboxStatus.Published p -> {
        publishedAt = p.at().toString();
        publishedBy = p.by().value();
      }
      case OutboxStatus.Quarantined q -> {
        quarantinedAt = q.at().toString();
        switch (q.reason()) {
          case QuarantineReason.MaxAttemptsExceeded max -> {
            quarantineKind = "MAX_ATTEMPTS";
            quarantineAttempts = max.attempts();
            quarantineText = max.lastError();
          }
          case QuarantineReason.PermanentFailure perm -> {
            quarantineKind = "PERMANENT";
            quarantineText = perm.reason();
          }
        }
      }
    }
    setNullableString(ps, i++, leaseOwner);
    setNullableInt(ps, i++, leaseToken);
    setNullableString(ps, i++, leaseExpires);
    setNullableString(ps, i++, publishedAt);
    setNullableString(ps, i++, publishedBy);
    setNullableString(ps, i++, quarantineKind);
    setNullableInt(ps, i++, quarantineAttempts);
    setNullableString(ps, i++, quarantineText);
    setNullableString(ps, i++, quarantinedAt);
    return i;
  }

  static OutboxMessage read(ResultSet rs) throws SQLException {
    byte[] bytes = rs.getBytes("payload");
    Instant next = Instant.ofEpochSecond(rs.getLong("next_attempt_s"), rs.getInt("next_attempt_n"));
    String status = rs.getString("status");
    OutboxStatus decoded =
        switch (status) {
          case "PENDING" -> new OutboxStatus.Pending();
          case "IN_FLIGHT" ->
              new OutboxStatus.InFlight(
                  new Lease(
                      new RelayId(required(rs, "lease_owner")),
                      rs.getInt("lease_token"),
                      Instant.parse(required(rs, "lease_expires_at"))));
          case "PUBLISHED" ->
              new OutboxStatus.Published(
                  Instant.parse(required(rs, "published_at")),
                  new RelayId(required(rs, "published_by")));
          case "QUARANTINED" ->
              new OutboxStatus.Quarantined(
                  quarantineReason(rs), Instant.parse(required(rs, "quarantined_at")));
          default -> throw new StorageException("unknown status " + status);
        };
    return new OutboxMessage(
        new MessageId(rs.getString("id")),
        rs.getString("aggregate_id"),
        rs.getString("type"),
        Payload.of(bytes),
        rs.getInt("attempts"),
        decoded,
        next,
        Instant.parse(rs.getString("created_at")));
  }

  private static QuarantineReason quarantineReason(ResultSet rs) throws SQLException {
    String kind = required(rs, "quarantine_kind");
    return switch (kind) {
      case "MAX_ATTEMPTS" ->
          new QuarantineReason.MaxAttemptsExceeded(
              rs.getInt("quarantine_attempts"), required(rs, "quarantine_text"));
      case "PERMANENT" -> new QuarantineReason.PermanentFailure(required(rs, "quarantine_text"));
      default -> throw new StorageException("unknown quarantine kind " + kind);
    };
  }

  private static String required(ResultSet rs, String column) throws SQLException {
    String value = rs.getString(column);
    if (value == null) {
      throw new StorageException("corrupt row: " + column + " is null");
    }
    return value;
  }

  private static String statusName(OutboxStatus status) {
    return switch (status) {
      case OutboxStatus.Pending p -> "PENDING";
      case OutboxStatus.InFlight f -> "IN_FLIGHT";
      case OutboxStatus.Published p -> "PUBLISHED";
      case OutboxStatus.Quarantined q -> "QUARANTINED";
    };
  }

  private static void setNullableString(PreparedStatement ps, int i, @Nullable String v)
      throws SQLException {
    if (v == null) {
      ps.setNull(i, Types.VARCHAR);
    } else {
      ps.setString(i, v);
    }
  }

  private static void setNullableInt(PreparedStatement ps, int i, @Nullable Integer v)
      throws SQLException {
    if (v == null) {
      ps.setNull(i, Types.INTEGER);
    } else {
      ps.setInt(i, v);
    }
  }
}
