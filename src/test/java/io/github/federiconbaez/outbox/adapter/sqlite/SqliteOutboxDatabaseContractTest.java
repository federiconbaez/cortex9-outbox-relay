package io.github.federiconbaez.outbox.adapter.sqlite;

import io.github.federiconbaez.outbox.adapter.contract.OutboxStoreContract;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

class SqliteOutboxDatabaseContractTest extends OutboxStoreContract {
  @TempDir Path dir;
  private SqliteOutboxDatabase db;

  @Override
  protected Subject create() {
    db = SqliteOutboxDatabase.open(dir.resolve("outbox.db"));
    return new Subject(db, db);
  }

  @AfterEach
  void close() {
    db.close();
  }
}
