package io.github.federiconbaez.outbox.application.port;

import java.time.Instant;

/** Fuente de tiempo. En producción conviene usar el reloj de la base compartida. */
public interface Clock {
  Instant now();
}
