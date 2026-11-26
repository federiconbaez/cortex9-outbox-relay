package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.adapter.contract.OutboxStoreContract;

class InMemoryOutboxDatabaseContractTest extends OutboxStoreContract {
  @Override
  protected Subject create() {
    InMemoryOutboxDatabase db = new InMemoryOutboxDatabase();
    return new Subject(db, db);
  }
}
