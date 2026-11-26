/**
 * Adapter durable sobre SQLite (JDBC). Implementa {@code UnitOfWork}, {@code OutboxStore} y {@code
 * DedupeStore} con transacciones {@code BEGIN IMMEDIATE}: el lock de escritura de SQLite da la
 * atomicidad entre hilos y entre procesos que comparten el archivo. Es el único paquete que conoce
 * {@code java.sql}.
 */
@NullMarked
package io.github.federiconbaez.outbox.adapter.sqlite;

import org.jspecify.annotations.NullMarked;
