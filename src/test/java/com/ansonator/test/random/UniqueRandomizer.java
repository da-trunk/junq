package com.ansonator.test.random;

import com.github.javafaker.Faker;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jeasy.random.randomizers.FakerBasedRandomizer;

/**
 * This is intended to guarantee that all created instances are unique. It's close, but isn't
 * actually doing that now. So, it's currently only used as a marker to identify code where we
 * require a uniqueness.
 *
 * @param <T>
 */
public abstract class UniqueRandomizer<T> extends FakerBasedRandomizer<T> implements Supplier<T> {
  protected static final Logger LOGGER = LogManager.getLogger();

  protected UniqueRandomizer() {
    super(0);
  }

  @Override
  public final T getRandomValue() {
    final T result;
    result = get();
    assert (result != null);
    return result;
  }

  public static <T> UniqueRandomizer<T> create(Supplier<T> randomizer) {
    return new UniqueRandomizer<T>() {
      @Override
      public T get() {
        return randomizer.get();
      }
    };
  }

  public static <T> UniqueRandomizer<T> createFromFaker(Function<Faker, T> randomizer) {
    return new UniqueRandomizer<T>() {
      @Override
      public T get() {
        return randomizer.apply(faker);
      }
    };
  }

  public static <T> UniqueRandomizer<T> createFromRandom(Function<Random, T> randomizer) {
    return new UniqueRandomizer<T>() {
      @Override
      public T get() {
        return randomizer.apply(random);
      }
    };
  }
}
