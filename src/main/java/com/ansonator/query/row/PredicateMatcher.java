package com.ansonator.query.row;

import java.util.Map;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
public class PredicateMatcher<T> implements RowMatcher {
  private final String name;
  private Predicate<T> pred;

  @SuppressWarnings("unchecked")
  @Override
  public boolean test(Row row, Map<String, Integer> columnByIndex) {
    final Integer index = columnByIndex.get(name);
    if (index == null) {
      throw new IllegalArgumentException(String.format("Column [%s] not found", name));
    }
    return pred.test((T) row.vals[index]);
  }
}
