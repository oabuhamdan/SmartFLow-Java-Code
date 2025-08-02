package edu.uta.flowsched;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchAwareBlockingQueue<T> extends LinkedBlockingQueue<T> {
    private final AtomicInteger remainingInBatch = new AtomicInteger(0);
    private volatile CountDownLatch batchCompleteLatch;
    private volatile boolean batchInProgress = false;

    public void startBatch(int batchSize) {
        remainingInBatch.set(batchSize);
        CountDownLatch newLatch = new CountDownLatch(1);
        batchCompleteLatch = newLatch;
        if (batchSize <= 0) {
            batchInProgress = false;
            newLatch.countDown();
        } else {
            batchInProgress = true;
        }
    }

    public boolean isBatchInProgress() {
        return batchInProgress;
    }

    @Override
    public boolean offer(T e) {
        boolean added = super.offer(e);
        if (added) {
            int left = remainingInBatch.decrementAndGet();
            if (left == 0) {
                batchInProgress = false;
                batchCompleteLatch.countDown();
            }
        }

        return added;
    }

    @Override
    public void put(T e) throws InterruptedException {
        super.put(e);
        int left = remainingInBatch.decrementAndGet();
        if (left == 0) {
            batchInProgress = false;
            batchCompleteLatch.countDown();
        }
    }

    @Override
    public T take() throws InterruptedException {
        // Only wait if there is a batch in progress AND the queue is currently empty
        waitForBatchIfInProgress();
        return super.take();
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        try {
            waitForBatchIfInProgress();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return super.drainTo(c);
    }

    private void waitForBatchIfInProgress() throws InterruptedException {
        CountDownLatch latch = batchCompleteLatch;
        if (latch != null && batchInProgress) {
            latch.await();
        }
    }

//    public synchronized void resizeBatch(int newSize) {
//        int currentRemaining = remainingInBatch.get();
//        int delta = newSize - currentRemaining;
//
//        if (!batchInProgress || batchCompleteLatch == null) {
//            throw new IllegalStateException("No batch in progress to resize.");
//        }
//
//        if (delta < 0) {
//            // Simulate completion if the remaining count becomes zero or less
//            int updatedRemaining = remainingInBatch.addAndGet(delta);
//            if (updatedRemaining <= 0) {
//                remainingInBatch.set(0);
//                batchInProgress = false;
//                batchCompleteLatch.countDown();
//            }
//        } else {
//            // Increase the batch size
//            remainingInBatch.addAndGet(delta);
//        }
//    }
}
