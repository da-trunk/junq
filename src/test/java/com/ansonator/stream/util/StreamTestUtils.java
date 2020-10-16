package com.ansonator.stream.util;

import java.util.function.IntFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.jooq.lambda.tuple.Tuple2;
import org.junit.jupiter.params.provider.Arguments;

public class StreamTestUtils {
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
    
    public static <A,B> Arguments argumentsOf2(Tuple2<A,B> in) {
        return Arguments.of(in.v1, in.v2);
    }

    public static <A,B,C> Arguments argumentsOf3(Tuple2<Tuple2<A,B>,C> in) {
        return Arguments.of(in.v1.v1, in.v1.v2, in.v2);
    }

    public static <A,B,C,D> Arguments argumentsOf4(Tuple2<Tuple2<Tuple2<A,B>,C>,D> in) {
        return Arguments.of(in.v1.v1.v1, in.v1.v1.v2, in.v1.v2, in.v2);
    }
}
