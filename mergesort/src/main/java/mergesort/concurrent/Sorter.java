package mergesort.concurrent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Nikolay Kirdin 2016-07-16
 * @version 0.2.2
 */
public class Sorter implements Runnable {

    public static final int SUPPOSED_AVERAGE_STRING_LENGTH_IN_BYTES = 120;

    /*
     * Number of sorted chunks.
     */
    private final AtomicInteger numberOfSortedChunks = new AtomicInteger(0);

    /*
     * Set with threads for controlling state and health of threads.
     */
    private final Set<Thread> threadSet = ConcurrentHashMap
            .<Thread> newKeySet();

    /*
     * Semaphore for indicating that all chunks completely sorted.
     */
    private final Semaphore allChanksSortedSemaphore = new Semaphore(0);

    public Semaphore getAllChanksSortedSemaphore() {
        return allChanksSortedSemaphore;
    }

    public Set<Thread> getThreadSet() {
        return threadSet;
    }

    public int getNumberOfSortedChunks() {
        return numberOfSortedChunks.get();
    }

    public void sort(File chunkOfFile) throws IOException {

        List<String> stringList = new ArrayList<>((int) chunkOfFile.length()
                / SUPPOSED_AVERAGE_STRING_LENGTH_IN_BYTES);

        try (BufferedReader br = new BufferedReader(
                new FileReader(chunkOfFile))) {

            String inputLine;
            while ((inputLine = br.readLine()) != null) {
                stringList.add(inputLine);
            }
        }

        Collections.sort(stringList);

        try (BufferedWriter bw = new BufferedWriter(
                new FileWriter(chunkOfFile))) {
            for (String outputString : stringList) {
                bw.write(outputString);
                bw.newLine();
            }
        }
    }

    @Override
    public void run() {
        Thread thread = Thread.currentThread();
        synchronized (threadSet) {
            threadSet.add(thread);
        }

        Object mergerMonitor = Utils.getMergerMonitor();

        BlockingQueue<File> unsortedChunksQueue = Utils
                .getUnsortedChunksQueue();
        Queue<File> sortedChunksQueue = Utils.getSortedChunksQueue();

        while (Utils.numberOfSplittingIntervals.get() != numberOfSortedChunks
                .get()) {

            File chunk = null;

            try {
                chunk = unsortedChunksQueue.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
            }

            if (chunk != null) {
                try {
                    sort(chunk);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                sortedChunksQueue.offer(chunk);
                Utils.numberOfChunksForMerging.getAndIncrement();
                numberOfSortedChunks.getAndIncrement();
                synchronized(mergerMonitor) {
                    mergerMonitor.notifyAll();
                }
                if (Utils.isVerbose())
                    System.out.println("mergesort: " + new Date()
                            + " : Sorter sorted chunk" + chunk);
            }
        }

        if (Utils.numberOfSplittingIntervals.get() == numberOfSortedChunks
                .get()
                && Utils.allChunksSorted.compareAndSet(false, true)) {
            allChanksSortedSemaphore.release();
            if (Utils.isVerbose())
                System.out.println("mergesort: " + new Date()
                        + " : Sorter sorted all chunks");
        }

        synchronized (thread) {
            threadSet.remove(Thread.currentThread());
        }
    }
}
