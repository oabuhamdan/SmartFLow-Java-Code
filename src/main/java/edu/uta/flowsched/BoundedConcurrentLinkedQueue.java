package edu.uta.flowsched;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundedConcurrentLinkedQueue<K> extends ConcurrentLinkedDeque<K> {
    private final int maxSize;
    private final AtomicInteger currentSize = new AtomicInteger(0);

    public BoundedConcurrentLinkedQueue(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean offerLast(K k) {
        boolean added = super.offerLast(k);
        if (added) {
            if (currentSize.incrementAndGet() > maxSize)
                pollFirst();
        }
        return added;
    }

    @Override
    public K pollFirst() {
         K item = super.pollFirst();
         if (item != null){
             currentSize.decrementAndGet();
         }
         return item;
    }

    @Override
    public int size() {
        return currentSize.get();
    }
}
