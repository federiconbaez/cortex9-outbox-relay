package io.github.federiconbaez.outbox.adapter.inmemory;

class InMemoryOutboxDatabaseContractTest extends OutboxStoreContract {
  @Override
  protected Subject create() {
    InMemoryOutboxDatabase db = new InMemoryOutboxDatabase();
    return new Subject(db, db);
  }
}
