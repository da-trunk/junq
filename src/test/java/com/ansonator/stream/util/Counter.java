package com.ansonator.stream.util;

/**
 * This can be used inside lambdas. It cheats the effectively final requirement.
 */
public class Counter {
    private int count;
    
    public Counter() {
        count = 0;
    }
    
    public Counter(int initial) {
        count = initial;
    }

    public int increment() {
        return increment(1);
    }
    
    public int increment(int amount) {
        count += amount;
        return count;
    }

    public int get() {
        return count;
    }
    
    public void reset() {
        count = 0;
    }
}
