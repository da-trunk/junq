package com.ansonator.stream.util;

import java.util.HashMap;
import java.util.Map;

public class Counters {
    private Map<String, Counter> counters = new HashMap<>();

    public int increment(String key) {
        return increment(key, 1);
    }

    public int increment(String key, int amount) {
        return counters.computeIfAbsent(key, k -> new Counter())
            .increment(amount);
    }

    public int get(String key) {
        return counters.computeIfAbsent(key, k -> new Counter())
            .get();
    }

    public void reset() {
        for (Counter counter : counters.values()) {
            counter.reset();
        }
    }
}
