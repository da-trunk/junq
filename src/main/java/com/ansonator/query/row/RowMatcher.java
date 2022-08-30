package com.ansonator.query.row;

import java.util.Map;
import java.util.Objects;

@FunctionalInterface
public interface RowMatcher {
  boolean test(Row row, Map<String, Integer> columnByIndex);

  /**
   * Returns a composed predicate that represents a short-circuiting logical AND of this predicate
   * and another. When evaluating the composed predicate, if this predicate is {@code false}, then
   * the {@code other} predicate is not evaluated.
   *
   * <p>Any exceptions thrown during evaluation of either predicate are relayed to the caller; if
   * evaluation of this predicate throws an exception, the {@code other} predicate will not be
   * evaluated.
   *
   * @param other a predicate that will be logically-ANDed with this predicate
   * @return a composed predicate that represents the short-circuiting logical AND of this predicate
   *     and the {@code other} predicate
   * @throws NullPointerException if other is null
   */
  default RowMatcher and(RowMatcher other) {
    Objects.requireNonNull(other);
    return (Row row, Map<String, Integer> columnByIndex) ->
        test(row, columnByIndex) && other.test(row, columnByIndex);
  }

  /**
   * Returns a predicate that represents the logical negation of this predicate.
   *
   * @return a predicate that represents the logical negation of this predicate
   */
  default RowMatcher negate() {
    return (Row row, Map<String, Integer> columnByIndex) -> !test(row, columnByIndex);
  }

  /**
   * Returns a composed predicate that represents a short-circuiting logical OR of this predicate
   * and another. When evaluating the composed predicate, if this predicate is {@code true}, then
   * the {@code other} predicate is not evaluated.
   *
   * <p>Any exceptions thrown during evaluation of either predicate are relayed to the caller; if
   * evaluation of this predicate throws an exception, the {@code other} predicate will not be
   * evaluated.
   *
   * @param other a predicate that will be logically-ORed with this predicate
   * @return a composed predicate that represents the short-circuiting logical OR of this predicate
   *     and the {@code other} predicate
   * @throws NullPointerException if other is null
   */
  default RowMatcher or(RowMatcher other) {
    Objects.requireNonNull(other);
    Objects.requireNonNull(other);
    return (Row row, Map<String, Integer> columnByIndex) ->
        test(row, columnByIndex) || other.test(row, columnByIndex);
  }
}
