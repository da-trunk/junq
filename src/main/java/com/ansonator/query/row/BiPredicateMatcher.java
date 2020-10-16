package com.ansonator.query.row;

import com.ansonator.query.row.Row;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.function.BiPredicate;

@Getter @Setter     
@RequiredArgsConstructor(staticName = "of")
public class BiPredicateMatcher<T1, T2> implements RowMatcher {
    private final String name1;
    private final String name2;
    private final BiPredicate<T1, T2> pred;

    @SuppressWarnings("unchecked")
    @Override
    public boolean test(final Row row, final Map<String, Integer> columnByIndex) {
        final Integer index1 = columnByIndex.get(name1);
        if (index1 == null) {
            throw new IllegalArgumentException(String.format("Column [%s] not found", name1));
        }
        final Integer index2 = columnByIndex.get(name2);
        if (index2 == null) {
            throw new IllegalArgumentException(String.format("Column [%s] not found", name2));
        }
        return pred.test((T1) row.vals[index1], (T2) row.vals[index2]);
    }
}
