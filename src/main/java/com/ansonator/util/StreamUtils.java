package com.ansonator.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

/**
 * Static utility class for Stream methods.
 * 
 * <ul>
 *   <li>Some of these methods were added after JDK8. 
 *   <li>Methods to collect streams which should be closed after collecting.
 *
 * @author BA030483
 *
 */
public class StreamUtils {
    public static <T, R> R collectAndClose(Stream<T> stream, Collector<? super T, ?, R> collector) {
        try {
            return stream.collect(collector);
        } finally {
            stream.close();
        }
    }

    public static <T> List<T> asList(Stream<T> stream) {
        return collectAndClose(stream, Collectors.toList());
    }

    /**
     * Useful as a downstream collector.
     * 
     * @param <T>
     * @param createArray a Function<Integer, T> to create the array given its size.
     * @return a Collector into T[]
     */
    public static <T> Collector<T, ?, T[]> toArray(IntFunction<T[]> createArray) {
        return Collectors.collectingAndThen(Collectors.toList(), list -> list.toArray(createArray.apply(list.size())));
    }

    public static Collector<Long, ?, long[]> toLongArray() {
        return Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
            .mapToLong(Long::longValue)
            .toArray());
    }

    public static Collector<Long, ?, LongStream> toLongStream() {
        return Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
            .mapToLong(Long::longValue));
    }

    public static <K, V> Map<V, K> reverseMap(Map<K, V> in) {
        return in.entrySet()
            .stream()
            .collect(Collectors.toMap(Entry::getValue, Entry::getKey));
    }

    public static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key [%s]", u.toString()));
        };
    }

    // This will be added in JDK9 (see JDK-8050820)
    public static <T> Stream<T> optionalToStream(Optional<T> o) {
        return o.isPresent() ? Stream.of(o.get()) : Stream.empty();
    }

    public static Optional<Long> createOptional(boolean hasValue, LongSupplier getValue) {
        if (hasValue) {
            return Optional.of(getValue.getAsLong());
        } else {
            return Optional.empty();
        }
    }

    public static <T> boolean notNull(T input) {
        return input != null;
    }

    // See https://stackoverflow.com/questions/29334404/how-to-force-max-to-return-all-maximum-values-in-a-java-stream
    public static <T> Collector<T, ?, List<T>> minList(Comparator<? super T> comp) {
        return Collector.of(ArrayList::new, (list, t) -> {
            int c;
            if (list.isEmpty() || ((c = comp.compare(t, list.get(0))) == 0)) {
                list.add(t);
            } else if (c < 0) {
                list.clear();
                list.add(t);
            }
        }, (list1, list2) -> {
            if (list1.isEmpty()) {
                return list2;
            }
            if (list2.isEmpty()) {
                return list1;
            }
            int r = comp.compare(list1.get(0), list2.get(0));
            if (r < 0) {
                return list1;
            } else if (r > 0) {
                return list2;
            } else {
                list1.addAll(list2);
                return list1;
            }
        });
    }

    // This will be available on Optional<T> in JDK9.
    public static <T> void ifPresentOrElse(java.util.Optional<T> optional, Consumer<? super T> presentAction, Runnable emptyAction) {
        if (optional.isPresent()) {
            presentAction.accept(optional.get());
        } else {
            emptyAction.run();
        }
    }

    public static <T, K, V> MultimapCollector<T, K, V> toMultimap(Function<T, K> keyGetter, Function<T, V> valueGetter) {
        return new MultimapCollector<>(keyGetter, valueGetter);
    }

    public static <T, K, V> MultimapCollector<T, K, T> toMultimap(Function<T, K> keyGetter) {
        return new MultimapCollector<>(keyGetter, v -> v);
    }

    // https://stackoverflow.com/questions/23003542/cleanest-way-to-create-a-guava-multimap-from-a-java8-stream
    public static class MultimapCollector<T, K, V> implements Collector<T, Multimap<K, V>, Multimap<K, V>> {

        private final Function<T, K> keyGetter;
        private final Function<T, V> valueGetter;

        public MultimapCollector(Function<T, K> keyGetter, Function<T, V> valueGetter) {
            this.keyGetter = keyGetter;
            this.valueGetter = valueGetter;
        }

        @Override
        public Supplier<Multimap<K, V>> supplier() {
            return ArrayListMultimap::create;
        }

        @Override
        public BiConsumer<Multimap<K, V>, T> accumulator() {
            return (map, element) -> map.put(keyGetter.apply(element), valueGetter.apply(element));
        }

        @Override
        public BinaryOperator<Multimap<K, V>> combiner() {
            return (map1, map2) -> {
                map1.putAll(map2);
                return map1;
            };
        }

        @Override
        public Function<Multimap<K, V>, Multimap<K, V>> finisher() {
            return map -> map;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return ImmutableSet.of(Characteristics.IDENTITY_FINISH);
        }
    }
}
