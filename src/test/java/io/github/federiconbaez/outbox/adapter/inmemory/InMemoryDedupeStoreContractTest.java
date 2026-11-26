package io.github.federiconbaez.outbox.adapter.inmemory;

import io.github.federiconbaez.outbox.adapter.contract.DedupeStoreContract;
import io.github.federiconbaez.outbox.application.port.DedupeStore;

class InMemoryDedupeStoreContractTest extends DedupeStoreContract {
  @Override
  protected DedupeStore create() {
    return new InMemoryDedupeStore();
  }
}
