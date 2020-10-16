package com.ansonator.util;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MergeUtils {

    /**
     * Concatenates two streams of equal type.  When
     *
     * other to our Streamthis stream If the specified key is not already associated with a value or is associated with
     * null, associates it with the given non-null value. Otherwise, replaces the associated value with the results of
     * the given remapping function, or removes if the result is {@code null}. This method may be of use when combining
     * multiple mapped values for a key. For example, to either create or append a {@code String msg} to a value
     * mapping:
     *
     * <pre> {@code
     * map.merge(key, msg, String::concat)
     * }</pre>
     *
     * <p>If the remapping function returns {@code null}, the mapping is removed.
     * If the remapping function itself throws an (unchecked) exception, the exception is rethrown, and the current
     * mapping is left unchanged.
     *
     * <p>The remapping function should not modify this map during computation.
     *
     * @param key               key with which the resulting value is to be associated
     * @param value             the non-null value to be merged with the existing value associated with the key or, if
     *                          no existing value or a null value is associated with the key, to be associated with the
     *                          key
     * @param remappingFunction the remapping function to recompute a value if present
     * @return the new value associated with the specified key, or null if no value is associated with the key
     * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
     *                                       href="{@docRoot}/java.base/java/util/Collection
     *                                       .html#optional-restrictions">optional</a>)
     * @throws ClassCastException            if the class of the specified key or value prevents it from being stored in
     *                                       this map (<a href="{@docRoot}/java.base/java/util/Collection
     *                                       .html#optional-restrictions">optional</a>)
     * @throws IllegalArgumentException      if some property of the specified key or value prevents it from being
     *                                       stored in this map (<a href="{@docRoot}/java.base/java/util/Collection
     *                                       .html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified key is null and this map does not support null keys or the
     *                                       value or remappingFunction is null
     * @implSpec The default implementation is equivalent to performing the following steps for this {@code map}, then
     * returning the current value or {@code null} if absent:
     *
     * <pre> {@code
     * V oldValue = map.get(key);
     * V newValue = (oldValue == null) ? value :
     *              remappingFunction.apply(oldValue, value);
     * if (newValue == null)
     *     map.remove(key);
     * else
     *     map.put(key, newValue);
     * }</pre>
     *
     * <p>The default implementation makes no guarantees about detecting if the
     * remapping function modifies this map during computation and, if appropriate, reporting an error. Non-concurrent
     * implementations should override this method and, on a best-effort basis, throw a {@code
     * ConcurrentModificationException} if it is detected that the remapping function modifies this map during
     * computation. Concurrent implementations should override this method and, on a best-effort basis, throw an {@code
     * IllegalStateException} if it is detected that the remapping function modifies this map during computation and as
     * a result computation would never complete.
     *
     * <p>The default implementation makes no guarantees about synchronization
     * or atomicity properties of this method. Any implementation providing atomicity guarantees must override this
     * method and document its concurrency properties. In particular, all implementations of subinterface {@link
     * java.util.concurrent.ConcurrentMap} must document whether the remapping function is applied once atomically only
     * if the value is not present.
     * @since 1.8
     */
    <T, K> Stream<T> concat(final Stream<T> left, final Stream<T> right, final Function<T, K> extractKey,
                            final BiFunction<? super T, ? super T, ? extends T> mergeFn) {
        final Set<K> leftKeys = removeNulls(left).map(extractKey).collect(Collectors.toSet());
        final Map<K, T> rightMap = removeNulls(right).collect(Collectors.toMap(extractKey, v -> v));
        final Stream<T> leftJoin = removeNulls(left).map(t1 -> {
            K k = extractKey.apply(t1);
            if (rightMap.containsKey(k)) {
                T t2 = rightMap.get(k);
                return mergeFn.apply(t1, t2);
            } else {
                return t1;
            }
        });
        final Stream<T> extra = removeNulls(right).filter(t2 -> !leftKeys.contains(extractKey.apply(t2)));
        return Stream.concat(leftJoin, extra);
    }

    <T, K> Stream<? extends T> join(final Stream<? extends T> left, final Stream<? extends T> right, final Function<T
            , K> extractKey, final BiFunction<? super T, ? super T, ? extends T> mergeFn) {
        final Map<K, T> rightMap = removeNulls(right).collect(Collectors.toMap(extractKey, v -> v));
        return removeNulls(left).filter(t -> t != null).map(t1 -> {
            K k = extractKey.apply(t1);
            if (rightMap.containsKey(k)) {
                T t2 = rightMap.get(k);
                return mergeFn.apply(t1, t2);
            } else {
                return null;
            }
        }).filter(t -> t != null);
    }

    private <T> Stream<T> removeNulls(Stream<T> in) {
        return in.filter(t -> t != null);
    }
}
