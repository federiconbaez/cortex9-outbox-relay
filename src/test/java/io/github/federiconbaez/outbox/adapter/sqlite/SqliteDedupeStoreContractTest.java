package io.github.federiconbaez.outbox.adapter.sqlite;

import io.github.federiconbaez.outbox.adapter.contract.DedupeStoreContract;
import io.github.federiconbaez.outbox.application.port.DedupeStore;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

class SqliteDedupeStoreContractTest extends DedupeStoreContract {
  @TempDir Path dir;
  private SqliteDedupeStore store;

  @Override
  protected DedupeStore create() {
    store = SqliteDedupeStore.open(dir.resolve("dedupe.db"));
    return store;
  }

  @AfterEach
  void close() {
    store.close();
  }
}
