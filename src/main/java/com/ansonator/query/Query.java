package com.ansonator.query;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.ansonator.query.cell.Aggregator;
import com.ansonator.query.row.BiPredicateMatcher;
import com.ansonator.query.row.PredicateMatcher;
import com.ansonator.query.row.Row;
import com.ansonator.query.row.RowMatcher;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Type-unsafe SQL-like queries for collections. This class is typically constructed from a concrete type via reflection
 * ({@link #from(Stream, Class)}. Also, its final result is typically collected into a concrete type via reflection
 * (i.e., @link #stream(Class, Supplier)}. But, it does not maintain type information during internal operations. It is
 * the client's responsibility to ensure that all column names map to a consistent type.
 *
 * @author Bryce Anson
 * @implSepc Internally, this class does not maintain type information. Instead, it only maintains a Stream of {@code
 * Object[]} via class {@link Row} and a mapping of column names to their index in this array. Because of this,
 * methods (such as {@link #unionAll}) which accept another Query as argument are unable to verify that their argument
 * is valid. They can only verify that the columns line up.
 * <p>
 * An alternative implementation would parameterize this type and save the {@code Class<T>} used to construct it. Then,
 * {@link #unionAll} could know that it is operating on a {@code Query<T>} constructed from the same type. However, this
 * would also require methods which modify {@link #getSelectedColumns} (such as {@link #join} and {@link #select}) to
 * provide the destination type. Requiring that would burden the client with a need to implement a POJO class for every
 * {@link #select} and {@link #join} result. In general, the client will only want to do this for the final result
 * (i.e., the one returned from {@link #stream(Class, Supplier)}). For this reason, we don't maintain a full set of type
 * information. It is the client's responsibility to use caution when invoking methods which accept another Query as
 * argument.
 * </p>
 */
@Log4j2
public class Query {
    private Stream<Row> data;
    private final Map<String, Integer> selectedColumns = new LinkedHashMap<>();

//    public interface Input {
//        default Row toRow() {
//            Map<String, Method> getters = new LinkedHashMap<>();
//            int i = 0;
//            for (Field field : fromType.getDeclaredFields()) {
//                try {
//                    Method getter = fromType.getMethod(toGetMethodName(field));
////                Method setter = fromType.getMethod(toSetMethodName(field), field.getType());
//                    getters.put(field.getName(), getter);
//                    selectedColumns.put(field.getName(), i);
//                    i++;
//                } catch (NoSuchMethodException | SecurityException e) {
//                    log.warn("Ignoring field [{}] of class [{}] as it lacks methods [{}()] and/or [{}({})] ",
//                            field.getName(),
//                            fromType.getName(), toGetMethodName(field), toSetMethodName(field), field.getType()
//                                                                                                     .getSimpleName());
//                }
//            }
//            this.data = data.map(item -> getters.values()
//                                                .stream()
//                                                .map(getter -> {
//                                                    try {
//                                                        Object val = getter.invoke(item);
//                                                        if (val == null) {
//                                                            log.warn("Getter [{}] return null for item {}",
//                                                                    getter.getName(), item);
//                                                        }
//                                                        return val;
//                                                    } catch (SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
//                                                        throw new IllegalArgumentException(String.format("Unable to "
//                                                                + "call method [%s]", getter.getName()));
//                                                    }
//                                                }))
//                            .map(Row::new);
//        }
//    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    /**
     * @return a copy of the column to index mapping.
     */
    public LinkedHashMap<String, Integer> getSelectedColumns() {
        return this.selectedColumns.entrySet()
                                   .stream()
                                   .collect(Collectors.toMap(Entry::getKey, Entry::getValue, throwingMerger(),
                                           LinkedHashMap::new));
    }

    public Object get(Row row, String column) {
        int index = selectedColumns.get(column);
        return row.vals[index];
    }

    // from

    public static <T> Query from(Stream<T> data, Class<T> clazz) {
        return new Query(data, clazz);
    }

    // select

    public Query select(String... columns) {
        final int[] indexes = Stream.of(columns)
                                    .mapToInt(getColumnIndex(selectedColumns))
                                    .toArray();
        data = data.map(row -> new Row(row, indexes));
        selectedColumns.clear();
        int i = 0;
        for (String column : columns) {
            selectedColumns.put(column, i);
            i++;
        }
        return this;
    }

    // join

    @AllArgsConstructor(staticName = "of")
    private static class RowPair {
        public final Row left;
        public final Row right;

        private Row concat() {
            return new Row(Stream.concat(Stream.of(left.vals), Stream.of(right.vals))
                                 .toArray());
        }
    }

    /**
     * Cartesian join two Query results. Follow this with {@link RowMatcher} factory methods like {@link #on} or {@link
     * #using} to perform a more restricted join.
     *
     * @param other This Query is fully loaded into memory.
     * @return Query
     */
    public Query join(final Query other) {
        // Load the right table into memory
        List<Row> otherData = other.data.collect(Collectors.toList());

        // Perform the Cartesian join
        data = data.flatMap(leftRow -> otherData.stream()
                                                .map(rightRow -> new RowPair(leftRow, rightRow)))
                   .map(RowPair::concat);

        // Modify column names contributed by right table when they are identical to a column from the left table.
        Set<String> commonColumns = Sets.intersection(selectedColumns.keySet(), other.selectedColumns.keySet());
        Map<String, Integer> newRightColumns = other.selectedColumns.entrySet()
                                                                    .stream()
                                                                    .collect(Collectors.toMap(e ->
                                                                                    commonColumns.contains(e.getKey()) ?
                                                                                            "right_" + e.getKey() :
                                                                                            e.getKey(),
                                                                            e -> e.getValue()
                                                                                    + selectedColumns.size()));
        selectedColumns.putAll(newRightColumns);
        return this;
    }

    /**
     * For each row in the input, execute the provided {@code dataFactory} and {@code clazz} to construct a new {@link
     * Query}. Combine these results into a single Query via {@link #unionAll}.
     *
     * @param <O>         Type containing fields corresponding to this join operation's result.
     * @param dataFactory Factory method to generate a Query from an input Row
     * @param clazz       class with fields matching the columns in our resultant Query
     * @return Query with columns matching the fields of otherType
     */
    public <O> Query join(Function<Row, Stream<O>> dataFactory, Class<O> clazz) {
        return data.map(row -> from(dataFactory.apply(row), clazz))
                   .reduce((a, b) -> a.unionAll(b))
                   .get();
    }

    /**
     * Identical to {@code where(Query.colEqual(left, right))}
     *
     * @param left
     * @param right
     * @return Query
     */
    public Query on(String left, String right) {
        return where(colEqual(left, right));
    }

    /**
     * This is identical to {@link #on} with the addition that it removes the "right_.." version of each joined column.
     * <p>
     * Assumes existence of two columns for each column provided. The second is expected to be named equal to the first,
     * aside from an extra prefix "right_".
     *
     * @param first
     * @param others
     * @return Query
     */
    public Query using(String first, String... others) {
        final RowMatcher matcher = Stream.concat(Stream.of(first), Stream.of(others))
                                         .map(col -> BiPredicateMatcher.of(col, "right_" + col, (a, b) -> a.equals(b)))
                                         .map(op -> (RowMatcher) op)
                                         .reduce((RowMatcher a, RowMatcher b) -> a.and(b))
                                         .get();
        final String[] newColumns = getSelectedColumns().keySet()
                                                        .stream()
                                                        .filter(col -> !col.startsWith("right_"))
                                                        .toArray(size -> new String[size]);
        return where(matcher).select(newColumns);
    }

    // where

    public static Predicate<Object> isTrue() {
        return obj -> obj.equals(true);
    }

    public static RowMatcher isTrue(String col) {
        return PredicateMatcher.of(col, isTrue());
    }

    public static RowMatcher colEqual(String col1, String col2) {
        return BiPredicateMatcher.of(col1, col2, (a, b) -> a.equals(b));
    }

    public static <T> RowMatcher isEqual(String col1, T val) {
        return PredicateMatcher.of(col1, (a) -> a.equals(val));
    }

    public Query where(final RowMatcher matcher) {
        final Map<String, Integer> selectedColumnsCopy = getSelectedColumns();
        data = data.filter(row -> matcher.test(row, selectedColumnsCopy));
        return this;
    }

    /**
     * TODO: {@link #groupBy} and {@link #orderBy} should be done this way too. {@link Aggregator} should extend
     * RowAggregator and orderBy should accept an array of RowComparator..
     *
     * @author BA030483
     */
    public Query where(final RowMatcher first, final RowMatcher... others) {
        final Map<String, Integer> selectedColumnsCopy = getSelectedColumns();
        RowMatcher matcher = Stream.concat(Stream.of(first), Stream.of(others))
                                   .reduce((a, b) -> a.and(b))
                                   .get();
        data = data.filter(row -> matcher.test(row, selectedColumnsCopy));
        return this;
    }

    // groupBy

    private Query groupBy(String[] columns, Map<String, BinaryOperator<Object>> accumulators) {
        final Map<String, Integer> selectedColumnsCopy = getSelectedColumns();
        Map<Integer, BinaryOperator<Object>> accumulatorsByIndex = accumulators.entrySet()
                                                                               .stream()
                                                                               .collect(Collectors.toMap(e -> selectedColumnsCopy.get(e.getKey()), e -> e.getValue()));
        BiConsumer<Row, Row> fn = (f, r) -> f.merge(r, accumulatorsByIndex);
        BinaryOperator<Row> op = (f1, f2) -> {
            f1.merge(f2, accumulatorsByIndex);
            return f1;
        };
        Collector<Row, Row, Row> downstream = Collector.of(Row::new, fn, op);
        Map<Row, Row> grouped = data.collect(Collectors.groupingBy(row -> select(row, columns), downstream));
        data = grouped.values()
                      .stream();
        return this;
    }

    public Query groupBy(String[] groupingColumns, Aggregator<?>... aggregators) {
        Map<String, BinaryOperator<Object>> accumulators = Stream.of(aggregators)
                                                                 .collect(Collectors.toMap(Aggregator::getName,
                                                                         Aggregator::asOperator));
        return groupBy(groupingColumns, accumulators);
    }

    public Query groupBy(String col1, Aggregator<?>... aggregators) {
        return groupBy(new String[]{col1}, aggregators);
    }

    public Query groupBy(String col1, String col2, Aggregator<?>... aggregators) {
        return groupBy(new String[]{col1, col2}, aggregators);
    }

    // orderBy

    public Query orderBy(Comparator<Row> comparator) {
        data = data.sorted(comparator);
        return this;
    }

    public Query orderBy(String first, String... others) {
        return orderBy(compareAsStrings(first, others));
    }

    @SafeVarargs
    public final Query orderBy(Comparator<Row> first, Comparator<Row>... others) {
        Comparator<Row> comparator = Stream.of(others)
                                           .reduce(first, (a, b) -> a.thenComparing(b));
        data = data.sorted(comparator);
        return this;
    }

    // union

    public Query unionAll(Query other) {
        LinkedHashMap<String, Integer> otherColumns = other.getSelectedColumns();
        if (!selectedColumns.equals(otherColumns)) {
            throw new IllegalArgumentException(String.format(
                    "Cannot union Query [%s] with Query [%s] because column sets are different.  Use select to limit "
                            + "columns first.",
                    selectedColumns.keySet(), otherColumns));
        }
        data = Stream.concat(data, other.data);
        return this;
    }

    public Query limit(int size) {
        data = data.limit(size);
        return this;
    }

    public Query log(Consumer<Row> action) {
        peek(action);
        return this;
    }

    public Query peek(Consumer<Row> action) {
        data = data.peek(action);
        return this;
    }

    public Stream<Row> stream() {
        return data;
    }

    public List<Row> list() {
        return data.collect(Collectors.toList());
    }

    public <O> List<O> list(Class<O> destType, Supplier<O> supplier) throws NoSuchFieldException, SecurityException {
        return stream(destType, supplier).collect(Collectors.toList());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////

    protected <T> Query(final Stream<T> data, final Class<T> fromType) {
        Map<String, Method> getters = new LinkedHashMap<>();
        int i = 0;
        for (Field field : fromType.getDeclaredFields()) {
            try {
                Method getter = fromType.getMethod(toGetMethodName(field));
//                Method setter = fromType.getMethod(toSetMethodName(field), field.getType());
                getters.put(field.getName(), getter);
                selectedColumns.put(field.getName(), i);
                i++;
            } catch (NoSuchMethodException | SecurityException e) {
                log.warn("Ignoring field [{}] of class [{}] as it lacks methods [{}()] and/or [{}({})] ",
                        field.getName(),
                        fromType.getName(), toGetMethodName(field), toSetMethodName(field), field.getType()
                                                                                                 .getSimpleName());
            }
        }
        this.data = data.map(item -> getters.values()
                                            .stream()
                                            .map(getter -> {
                                                try {
                                                    Object val = getter.invoke(item);
                                                    if (val == null) {
                                                        log.warn("Getter [{}] return null for item {}",
                                                                getter.getName(), item);
                                                    }
                                                    return val;
                                                } catch (SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                                                    throw new IllegalArgumentException(String.format("Unable to call "
                                                            + "method [%s]", getter.getName()));
                                                }
                                            }))
                        .map(Row::new);
    }

    private static String toGetMethodName(final Field field) {
        final String prefix;
        if (field.getType()
                 .equals(Boolean.class)
                || field.getType()
                        .equals(boolean.class)) {
            prefix = "is";
        } else {
            prefix = "get";
        }
        return prefix + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());
    }

    private static String toSetMethodName(final Field field) {
        final String prefix = "set";
        return prefix + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());
    }

    private static String toFieldName(final Method method) {
        if (method.getName()
                  .startsWith("is")) {
            return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, method.getName()
                                                                           .substring(2));
        }
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, method.getName()
                                                                       .substring(3));
    }

    /**
     * Use reflection to convert our {@code Stream<Row>} to a {@code Stream<T>}.
     *
     * @param <T>
     * @param destType The output type. This type must contain a setter for each currently selected column.
     * @param supplier The no-argument constructor for type T.
     * @return Stream<T>
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    public <T> Stream<T> stream(Class<T> destType, Supplier<T> supplier) throws NoSuchFieldException,
            SecurityException {
        try {
            destType.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(String.format(
                    "Type [%s] is invalid for method stream(destType, supplier).  It lacks a no-argument constructor"
                    , destType.getName()));
        }
        int lastIndex = selectedColumns.values()
                                       .stream()
                                       .mapToInt(Integer::intValue)
                                       .max()
                                       .orElseGet(() -> 0);
        Method[] setters = new Method[lastIndex + 1];
        for (Method method : destType.getMethods()) {
            if (method.getName()
                      .startsWith("set")) {
                String colName = toFieldName(method);
                Integer index = selectedColumns.get(colName);
                Parameter[] params = method.getParameters();
                if (index != null && params.length == 1) {
                    log.debug("Mapping column [{}] to setter [{}]", colName, method.getName());
                    setters[index] = method;
                }
            }
        }
        return data.map(row -> {
            T dest = supplier.get();
            for (int i = 0; i < row.vals.length; i++) {
                if (setters[i] != null) {
                    try {
                        setters[i].invoke(dest, row.vals[i]);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    final int iFinal = i;
                    List<String> colNames = selectedColumns.entrySet()
                                                           .stream()
                                                           .filter(e -> e.getValue()
                                                                         .intValue() == iFinal)
                                                           .map(Entry::getKey)
                                                           .map(col -> CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, col))
                                                           .collect(Collectors.toList());
                    String message
                            = String.format("Method set%s not found in type [%s].  Cannot assign Object from index %d"
                                    + " of row %s",
                            colNames.get(0), destType.getName(), i, row);
                    throw new RuntimeException(message);
                }
            }
            return dest;
        });
    }

    private Row select(final Row row, String... columns) {
        return new Row(Stream.of(columns)
                             .mapToInt(getColumnIndex(selectedColumns))
                             .mapToObj(i -> row.get(i)));
    }

    private Comparator<Row> compareAsStrings(String first, String... others) {
        return Stream.concat(Stream.of(first), Stream.of(others))
                     .mapToInt(getColumnIndex(selectedColumns))
                     .mapToObj(i -> {
                         Comparator<Row> comp = (Row row1, Row row2) -> (
                                 row1.get(i)
                                     .toString()
                         ).compareTo(row2.get(i)
                                         .toString());
                         return comp;
                     })
                     .reduce((a, b) -> a.thenComparing(b))
                     .get();
    }

    private static ToIntFunction<String> getColumnIndex(final Map<String, Integer> columns) {
        return column -> {
            Integer index = columns.get(column);
            if (index == null) {
                throw new IllegalArgumentException(String.format("Column [%s] not found", column));
            }
            return index;
        };
    }
}
