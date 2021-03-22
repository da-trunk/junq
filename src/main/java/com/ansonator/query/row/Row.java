package com.ansonator.query.row;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Row {
    public Object[] vals = null; // TODO: this should not be accessible outside this project.

    public Row() {
    }

    public static Row of(Object... vals) {
        return new Row(vals);
    }

    public Row(Stream<Object> vals) {
        this(vals.toArray());
    }

    public Row(Object[] vals) {
        this.vals = vals;
    }

    public Row(final Row other, final int[] indexes) {
        this.vals = IntStream.of(indexes)
                             .mapToObj(index -> other.vals[index])
                             .toArray();
    }

    public void add(final Row other, final int[] indexes) {
        this.vals = Stream.concat(Stream.of(vals), IntStream.of(indexes)
                                                            .mapToObj(index -> other.vals[index]))
                          .toArray();
    }

    public void merge(@Nonnull final Row other, Map<Integer, BinaryOperator<Object>> accumulators) { // TODO: this should not be used outside this project
        if (vals == null) {
            vals = other.vals;
        } else if (vals.length != other.vals.length) {
            throw new IllegalArgumentException("Cannot merge rows of different size");
        } else {
            for (int i = 0; i < vals.length; i++) {
                BinaryOperator<Object> accumulator = accumulators.get(i);
                if (accumulator != null) {
                    vals[i] = accumulator.apply(vals[i], other.vals[i]);
                } else if (!vals[i].equals(other.vals[i])) {
                    throw new IllegalArgumentException(
                            String.format("Cannot merge [%s] from row %s into [%s] from row %s without an "
                                            + "accumulator for column [%d]",
                                    other.vals[i], other, vals[i], this, i));
                }
            }
        }
    }

    @Override
    public String toString() {
        return "(" + Stream.of(vals)
                           .map(val -> val == null ? "null" : val.toString())
                           .map(str -> str.isEmpty() ? "\"\"" : str)
                           .collect(Collectors.joining(", ")) + ")";
    }

    public Object get(int i) {
        return vals[i];
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.deepHashCode(vals);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Row other = (Row) obj;
        if (Arrays.deepEquals(vals, other.vals)) {
            return true;
        }
        return false;
    }
}
