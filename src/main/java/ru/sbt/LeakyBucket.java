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

    LeakyBucket(Consumer<T> consumer, int storage, int rate, int size) {
        this.size = size;
        this.consumer = consumer;
        this.queue = new ArrayBlockingQueue<T>(storage);
        this.timer = new Timer();
        timer.scheduleAtFixedRate(timerTask(), 0L, rate);
    }

    private boolean push(T object) {
        return queue.offer(object);
    }

    private void pop() {
        List<T> toProcess = new ArrayList<>(size);
        queue.drainTo(toProcess);
        toProcess.forEach(consumer);
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
