package io.github.federiconbaez.outbox.application.port;

public interface Publisher {
  PublishResult publish(Envelope envelope);
}
