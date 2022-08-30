package com.ansonator.query.cell;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(staticName = "of")
public class Aggregator<T> {
  @Getter private final String name;
  private final BiConsumer<T, T> consumer;

  @SuppressWarnings("unchecked")
  public BinaryOperator<Object> asOperator() {
    return (Object a, Object b) -> {
      consumer.accept((T) a, (T) b);
      return a;
    };
  }

  @SuppressWarnings("unchecked")
  public BiConsumer<Object, Object> asConsumer() {
    return (Object a, Object b) -> consumer.accept((T) a, (T) b);
  }
}
