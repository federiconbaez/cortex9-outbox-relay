package io.github.federiconbaez.outbox.application.port;

import io.github.federiconbaez.outbox.domain.MessageId;

public interface IdGenerator {
  MessageId next();
}
