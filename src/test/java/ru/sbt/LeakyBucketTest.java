package ru.sbt;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.System.nanoTime;
import static java.lang.Thread.sleep;
import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.*;

public class LeakyBucketTest {
    private TimerTask producerFor(LeakyBucket<Integer> rateLimiter, Set<Integer> produced) {
        return new TimerTask() {
            @Override
            public void run() {
                Random rand = new Random();
                Integer value = rand.nextInt();
                rateLimiter.push(value);
                produced.add(value);
            }
        };
    }

    @Test
    void collectAllObjectsWhenNotOverloaded() throws InterruptedException {
        Set<Integer> consumed = new HashSet<>();
        Set<Integer> produced = new ConcurrentSkipListSet<>();

        Consumer<Integer> consumer = obj -> consumed.add(obj);
        LeakyBucket<Integer> rateLimiter = new LeakyBucket<>(consumer, 10, 20, 1);

        List<Timer> producers = IntStream.range(0, 3).mapToObj(i -> new Timer()).collect(Collectors.toList());
        producers.forEach(t -> t.scheduleAtFixedRate(producerFor(rateLimiter, produced), 0L, 100));
        sleep(2_000);
        producers.forEach(t -> t.cancel());
        sleep(200);

        assertEquals(produced.size(), consumed.size());
        assertTrue(produced.containsAll(consumed));
    }

    @Test
    void collectObjectsWhenOverloaded() throws InterruptedException {
        Set<Integer> consumed = new HashSet<>();
        Set<Integer> produced = new ConcurrentSkipListSet<>();

        Consumer<Integer> consumer = obj -> consumed.add(obj);
        LeakyBucket<Integer> rateLimiter = new LeakyBucket<>(consumer, 5, 100, 1);

        List<Timer> producers = IntStream.range(0, 3).mapToObj(i -> new Timer()).collect(Collectors.toList());
        producers.forEach(t -> t.scheduleAtFixedRate(producerFor(rateLimiter, produced), 0L, 50));
        sleep(2_000);
        producers.forEach(t -> t.cancel());
        sleep(200);

        assertNotEquals(produced.size(), consumed.size());
        assertTrue(produced.containsAll(consumed));
    }

    @Test
    void controlRate() throws InterruptedException {
        List<Long> consumedTimes = new ArrayList<>();
        Set<Integer> produced = new ConcurrentSkipListSet<>();

        Consumer<Integer> consumer = obj -> consumedTimes.add(System.currentTimeMillis());
        LeakyBucket<Integer> rateLimiter = new LeakyBucket<>(consumer, 10, 100, 1);

        Timer producer = new Timer();
        producer.scheduleAtFixedRate(producerFor(rateLimiter, produced), 0L, 10);
        sleep(2_000);
        producer.cancel();
        sleep(200);

        consumedTimes.stream().reduce((prev, next) -> {
            long rate = next - prev;
            assertTrue(rate > 90);
            assertTrue(rate < 110);
            return next;
        });
    }
}











