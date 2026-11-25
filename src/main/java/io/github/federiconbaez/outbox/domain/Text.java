package io.github.federiconbaez.outbox.domain;

import java.util.Objects;

final class Text {
  private Text() {}

  static void requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
