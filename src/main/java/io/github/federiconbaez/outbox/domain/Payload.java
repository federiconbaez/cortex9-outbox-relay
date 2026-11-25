package io.github.federiconbaez.outbox.domain;

import java.util.Arrays;
import java.util.Objects;

/** Bytes opacos del mensaje. Inmutable: copia defensiva al entrar y al salir. */
public final class Payload {
  private final byte[] bytes;

  private Payload(byte[] bytes) {
    this.bytes = bytes;
  }

  public static Payload of(byte[] bytes) {
    Objects.requireNonNull(bytes, "payload");
    return new Payload(bytes.clone());
  }

  public byte[] bytes() {
    return bytes.clone();
  }

  public int size() {
    return bytes.length;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Payload p && Arrays.equals(bytes, p.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    return "Payload[" + bytes.length + " bytes]";
  }
}
