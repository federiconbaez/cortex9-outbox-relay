package io.github.federiconbaez.outbox.adapter.inmemory;

/** Simula que el proceso murió en un punto exacto (inyección de fallos en tests). */
public final class SimulatedCrash extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public SimulatedCrash(String where) {
    super("simulated crash: " + where);
  }
}
