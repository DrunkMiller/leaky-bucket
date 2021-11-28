package ru.sbt;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;

public class LeakyBucket<T> {
    private final ArrayBlockingQueue<T> queue;
    private final Timer timer;

    private final Consumer<T> consumer;
    private final int size;
    private List<T> buffer;

    LeakyBucket(Consumer<T> consumer, int storage, long rate, int size) {
        this.size = size;
        this.consumer = consumer;
        buffer = new ArrayList<>(size);
        this.queue = new ArrayBlockingQueue<>(storage);
        this.timer = new Timer();
        timer.scheduleAtFixedRate(timerTask(), 0L, rate);
    }

    public boolean push(T object) {
        return queue.offer(object);
    }

    private void pop() {
        buffer.clear();
        queue.drainTo(buffer, size);
        buffer.forEach(consumer);
    }

    private TimerTask timerTask() {
        return new TimerTask() {
            @Override
            public void run() {
                pop();
            }
        };
    }
}
