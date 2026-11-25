package io.github.federiconbaez.outbox.application.port;

/** Azar inyectable para el jitter; con semilla fija los tests son deterministas. */
public interface RandomSource {
  /** Devuelve un valor en [0, boundExclusive). */
  long nextLong(long boundExclusive);
}
