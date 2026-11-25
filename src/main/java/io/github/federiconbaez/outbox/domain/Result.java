package io.github.federiconbaez.outbox.domain;

import java.util.Objects;

/** Resultado de una transición: el nuevo valor o un {@link DomainError}. */
public sealed interface Result<T> {

  static <T> Result<T> ok(T value) {
    return new Ok<>(value);
  }

  static <T> Result<T> err(DomainError error) {
    return new Err<>(error);
  }

  record Ok<T>(T value) implements Result<T> {
    public Ok {
      Objects.requireNonNull(value, "value");
    }
  }

  record Err<T>(DomainError error) implements Result<T> {
    public Err {
      Objects.requireNonNull(error, "error");
    }
  }
}
