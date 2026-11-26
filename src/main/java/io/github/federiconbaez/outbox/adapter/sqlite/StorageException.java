package io.github.federiconbaez.outbox.adapter.sqlite;

/**
 * Falla de infraestructura (I/O, base bloqueada más allá del timeout, esquema inválido). No es un
 * error de negocio: los errores de negocio siguen siendo {@code DomainError}.
 */
public final class StorageException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageException(String message) {
    super(message);
  }
}
