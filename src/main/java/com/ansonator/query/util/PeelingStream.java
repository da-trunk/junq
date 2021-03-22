package com.ansonator.query.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A wrapped Stream which implements Iterator and Iterable. Useful when you want to remove an element from the stream without terminating
 * it. See {@link #next}.
 * 
 * All terminal operations {@link #close} the wrapped Stream before returning.
 * 
 * @author BA030483
 *
 * @param <T>
 */
public class PeelingStream<T> implements Stream<T>, Iterator<T>, Iterable<T>, Cloneable {
    private static final Random rand = new Random();
    private static final int MAX_CLONEABLE_SIZE = 100;
    private Stream<T> wrapped;

    public PeelingStream(Stream<T> toBeWrapped) {
        this.wrapped = toBeWrapped;
    }

    /**
     * Retrieves the first element from the stream. Then, recreates the stream without this element.
     * 
     * @see <a href=
     *      "https://stackoverflow.com/questions/26595020/getting-the-next-item-from-a-java-8-stream">getting-the-next-item-from-a-java-8-stream</a>
     */
    @Override
    public T next() {
        Iterator<T> iterator = wrapped.iterator();
        T next = iterator.next();
        Iterable<T> remainingIterable = () -> iterator;
        wrapped = StreamSupport.stream(remainingIterable.spliterator(), false);

        return next;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    /**
     * @param <T>
     * @param values
     * @return an infinite Stream of the provided values selected at random.
     */
    public static <T> PeelingStream<T> random(final T[] values) {
        Stream<T> stream = Stream.generate(() -> values[rand.nextInt(values.length)]);
        return new PeelingStream<>(stream);
    }

    /**
     * @param <T>
     * @param values
     * @return an infinite Stream of the provided values selected at random.
     */
    public static <T> PeelingStream<T> random(final List<T> values) {
        Stream<T> stream = Stream.generate(() -> values.get(rand.nextInt(values.size())));
        return new PeelingStream<>(stream);
    }

    /**
     * @param <T>
     * @param values
     * @return an infinite Stream of the provided values selected at random.
     */
    public static <T> PeelingStream<T> random(final Collection<T> values) {
        return random(new ArrayList<>(values));
    }

    /**
     * @param bound
     * @return an infinite stream of random Integer bounded by bound.
     */
    public static PeelingStream<Integer> randomInts(int bound) {
        Stream<Integer> stream = Stream.generate(() -> rand.nextInt(bound));
        return new PeelingStream<>(stream);
    }

    public static PeelingStream<Integer> randomInts(long bound) {
        return randomInts((int) bound);
    }

    /**
     * @return an infinite stream of random Double.
     */
    public static PeelingStream<Double> randomDoubles() {
        Stream<Double> stream = Stream.generate(() -> rand.nextDouble());
        return new PeelingStream<>(stream);
    }

    /**
     * WARNING: avoid calling this without understanding the implications.
     *
     * This clones the stream by materializing it into an in memory-collection. It then uses the collection to recreate two new streams. One
     * is returned as the clone while the other is retained to avoid having the orignal wrapped stream be in a terminal state.
     *
     * So, this consumes twice as much memory as required to represent the original stream. Thus, it should always be called as late as
     * possible in the stream's composition.
     *
     * throws CloneNotSupportedException if the stream's size exceeds MAX_SIZE.
     */
    @Override
    public PeelingStream<T> clone() throws CloneNotSupportedException {
        List<T> list = wrapped.limit(MAX_CLONEABLE_SIZE + 1)
            .collect(Collectors.toList());
        if (list.size() > MAX_CLONEABLE_SIZE) {
            throw new CloneNotSupportedException("Unable to clone a stream of size greater than " + MAX_CLONEABLE_SIZE);
        }
        wrapped = list.stream();
        List<T> listCopy = new ArrayList<>(list);
        return new PeelingStream<>(listCopy.stream());
    }

    @Override
    public <R> PeelingStream<R> map(Function<? super T, ? extends R> mapper) {
        return new PeelingStream<>(wrapped.map(mapper));
    }

    @Override
    public <R> PeelingStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new PeelingStream<>(wrapped.flatMap(mapper));
    }

    ///////////////////// from here, only plain delegate methods but returning our type instead of Stream

    @Override
    public Iterator<T> iterator() {
        return wrapped.iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return wrapped.spliterator();
    }

    @Override
    public boolean isParallel() {
        return wrapped.isParallel();
    }

    @Override
    public PeelingStream<T> sequential() {
        wrapped = wrapped.sequential();
        return this;
    }

    @Override
    public PeelingStream<T> parallel() {
        wrapped = wrapped.parallel();
        return this;
    }

    @Override
    public PeelingStream<T> unordered() {
        wrapped = wrapped.unordered();
        return this;
    }

    @Override
    public PeelingStream<T> onClose(Runnable closeHandler) {
        wrapped = wrapped.onClose(closeHandler);
        return this;
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public PeelingStream<T> filter(Predicate<? super T> predicate) {
        wrapped = wrapped.filter(predicate);
        return this;
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return wrapped.mapToInt(mapper);
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return wrapped.mapToLong(mapper);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return wrapped.mapToDouble(mapper);
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return wrapped.flatMapToInt(mapper);
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return wrapped.flatMapToLong(mapper);
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return wrapped.flatMapToDouble(mapper);
    }

    @Override
    public PeelingStream<T> distinct() {
        wrapped = wrapped.distinct();
        return this;
    }

    @Override
    public PeelingStream<T> sorted() {
        wrapped = wrapped.sorted();
        return this;
    }

    @Override
    public PeelingStream<T> sorted(Comparator<? super T> comparator) {
        wrapped = wrapped.sorted(comparator);
        return this;
    }

    @Override
    public PeelingStream<T> peek(Consumer<? super T> action) {
        wrapped = wrapped.peek(action);
        return this;
    }

    @Override
    public PeelingStream<T> limit(long maxSize) {
        wrapped = wrapped.limit(maxSize);
        return this;
    }

    @Override
    public PeelingStream<T> skip(long n) {
        wrapped = wrapped.skip(n);
        return this;
    }

    //// Terminal operations

    @Override
    public void forEach(Consumer<? super T> action) {
        try {
            wrapped.forEach(action);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        try {
            wrapped.forEachOrdered(action);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public Object[] toArray() {
        try {
            return wrapped.toArray();
        } finally {
            wrapped.close();
        }
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        try {
            return wrapped.toArray(generator);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        try {
            return wrapped.reduce(identity, accumulator);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        try {
            return wrapped.reduce(accumulator);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        try {
            return wrapped.reduce(identity, accumulator, combiner);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        try {
            return wrapped.collect(supplier, accumulator, combiner);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        try {
            return wrapped.collect(collector);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        try {
            return wrapped.min(comparator);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        try {
            return wrapped.max(comparator);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public long count() {
        try {
            return wrapped.count();
        } finally {
            wrapped.close();
        }
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        try {
            return wrapped.anyMatch(predicate);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        try {
            return wrapped.allMatch(predicate);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        try {
            return wrapped.noneMatch(predicate);
        } finally {
            wrapped.close();
        }
    }

    @Override
    public Optional<T> findFirst() {
        try {
            return wrapped.findFirst();
        } finally {
            wrapped.close();
        }
    }

    @Override
    public Optional<T> findAny() {
        try {
            return wrapped.findAny();
        } finally {
            wrapped.close();
        }
    }

    @SuppressWarnings("unchecked")
    public Optional<T> findRandom() {
        final Object[] values = toArray();
        if (values.length > 0) {
            T value = (T) values[rand.nextInt(values.length)];
            return Optional.of(value);
        } else {
            return Optional.empty();
        }
    }
}
